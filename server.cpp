#include <winsock2.h>
#include <ws2tcpip.h>
#include <iostream>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <limits>
#include <string>

#pragma comment(lib, "ws2_32.lib")

#define SERVER_PORT 65001
#define LOG_PREFIX "[Server] "

// Protocol Commands
const uint32_t CMD_CONFIG_DATA = 1;
const uint32_t CMD_START_COMP = 2;
const uint32_t CMD_GET_STATUS = 3;
const uint32_t RESP_ACK = 10;
const uint32_t RESP_STATUS_PENDING = 11;
const uint32_t RESP_RESULT = 12;
const uint32_t RESP_ERROR = 13;

struct ClientState {
    SOCKET socket = INVALID_SOCKET;
    uint32_t matrixSize = 0;
    uint32_t numThreads = 1;
    std::vector<float> matrixData;
    std::vector<float> resultData;
    std::atomic<bool> dataReceived{false};
    std::atomic<bool> processingStarted{false}; // True if computation thread is active/launched
    std::atomic<bool> processingDone{false};   // True if computation finished successfully
    std::atomic<bool> errorOccurred{false};    // True if computation failed
    std::thread workerThread;

    // Destructor to ensure thread is joined if needed (basic safety)
    ~ClientState() {
        if (workerThread.joinable()) {
            // In a real-world scenario, you might need to signal the thread to stop first
            // For simplicity here, we just join, hoping it finishes.
            try { workerThread.join(); } catch(...) { /* ignore errors on cleanup */ }
        }
    }
};

// --- Helper Functions for Network I/O with Error Logging ---
std::string GetWSAErrorString(int errorCode) {
    char* s = nullptr;
    FormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
                   NULL, errorCode, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), (LPSTR)&s, 0, NULL);
    std::string msg = (s ? s : "Unknown error");
    if(s) LocalFree(s);
    return msg + " (" + std::to_string(errorCode) + ")";
}

bool send_uint32(SOCKET sock, uint32_t value, const std::string& context) {
    uint32_t netValue = htonl(value);
    int bytesSent = send(sock, (const char*)&netValue, sizeof(netValue), 0);
    if (bytesSent == SOCKET_ERROR) {
        int error_code = WSAGetLastError();
        std::cerr << LOG_PREFIX << "[" << sock << "] send_uint32 failed (" << context << "): " << GetWSAErrorString(error_code) << std::endl;
        return false;
    }
    if (bytesSent != sizeof(netValue)) {
        std::cerr << LOG_PREFIX << "[" << sock << "] send_uint32 sent incomplete data (" << context << "): " << bytesSent << "/" << sizeof(netValue) << std::endl;
        return false; // Treat incomplete send as error
    }
    return true;
}

bool recv_uint32(SOCKET sock, uint32_t& value, const std::string& context) {
    int bytesReceived = recv(sock, (char*)&value, sizeof(value), MSG_WAITALL); // Wait for exactly 4 bytes
    if (bytesReceived == sizeof(value)) {
        value = ntohl(value);
        return true;
    }
    if (bytesReceived == 0) {
        std::cerr << LOG_PREFIX << "[" << sock << "] recv_uint32 (" << context << "): Client disconnected gracefully." << std::endl;
    } else if (bytesReceived == SOCKET_ERROR) {
        int error_code = WSAGetLastError();
        std::cerr << LOG_PREFIX << "[" << sock << "] recv_uint32 failed (" << context << "): " << GetWSAErrorString(error_code) << std::endl;
        if (error_code == WSAECONNRESET) {
            // This specifically means the client likely crashed or force-closed
            std::cerr << LOG_PREFIX << "[" << sock << "] Connection reset by peer." << std::endl;
        }
    } else {
        std::cerr << LOG_PREFIX << "[" << sock << "] recv_uint32 received incomplete data (" << context << "): " << bytesReceived << "/" << sizeof(value) << std::endl;
    }
    return false; // Any outcome other than receiving exactly sizeof(value) bytes is failure
}

bool send_floats(SOCKET sock, const std::vector<float>& data, const std::string& context) {
    size_t totalBytes = data.size() * sizeof(float);
    if (totalBytes == 0) return true; // Nothing to send
    size_t bytesSent = 0;
    const char* buffer = reinterpret_cast<const char*>(data.data());
    while (bytesSent < totalBytes) {
        int result = send(sock, buffer + bytesSent, (int)(totalBytes - bytesSent), 0);
        if (result == SOCKET_ERROR) {
            int error_code = WSAGetLastError();
            std::cerr << LOG_PREFIX << "[" << sock << "] send_floats failed (" << context << "): " << GetWSAErrorString(error_code) << std::endl;
            return false;
        }
        bytesSent += result;
    }
    return true;
}

bool recv_floats(SOCKET sock, std::vector<float>& data, size_t count, const std::string& context) {
    if (count == 0) { data.clear(); return true; } // Nothing to receive
    data.resize(count); // Allocate space
    size_t totalBytes = count * sizeof(float);
    size_t bytesReceived = 0;
    char* buffer = reinterpret_cast<char*>(data.data());
    while (bytesReceived < totalBytes) {
        int result = recv(sock, buffer + bytesReceived, (int)(totalBytes - bytesReceived), 0); // Read remaining bytes
        if (result == SOCKET_ERROR) {
            int error_code = WSAGetLastError();
            std::cerr << LOG_PREFIX << "[" << sock << "] recv_floats failed (" << context << "): " << GetWSAErrorString(error_code) << std::endl;
            if (error_code == WSAECONNRESET) {
                std::cerr << LOG_PREFIX << "[" << sock << "] Connection reset by peer during float receive." << std::endl;
            }
            return false;
        }
        if (result == 0) {
            std::cerr << LOG_PREFIX << "[" << sock << "] recv_floats (" << context << "): Client disconnected before all data received." << std::endl;
            return false; // Client disconnected prematurely
        }
        bytesReceived += result;
    }
    return true;
}
// ---------------------------------------

// --- Matrix Processing Logic ---
void process_matrix_rows(std::vector<float>* matrixPtr, uint32_t size, int startRow, int endRow) {
    std::vector<float>& matrix = *matrixPtr;
    for (int i = startRow; i < endRow; ++i) {
        float maxVal = -std::numeric_limits<float>::infinity();
        size_t rowStartIndex = (size_t)i * size;
        for (uint32_t j = 0; j < size; ++j) {
            if (matrix[rowStartIndex + j] > maxVal) maxVal = matrix[rowStartIndex + j];
        }
        if (i < size) matrix[rowStartIndex + i] = maxVal;
    }
}

void perform_computation(ClientState* state) {
    // Run in a separate function to easily capture exceptions
    try {
        // Use resultData as the working copy
        if (state->resultData.size() != state->matrixData.size()) {
            state->resultData = state->matrixData; // Initial copy
        } else {
            std::copy(state->matrixData.begin(), state->matrixData.end(), state->resultData.begin()); // Recopy if needed
        }

        uint32_t size = state->matrixSize;
        uint32_t threads_to_use = std::max(1u, state->numThreads);
        std::vector<std::thread> workers;
        int rowsPerThread = size / threads_to_use;
        int extraRows = size % threads_to_use;
        int startRow = 0;

        for (uint32_t i = 0; i < threads_to_use; ++i) {
            int rowsForThisThread = rowsPerThread + (i < extraRows ? 1 : 0);
            if (rowsForThisThread > 0 && startRow < size) {
                int endRow = std::min((uint32_t)(startRow + rowsForThisThread), size);
                workers.emplace_back(process_matrix_rows, &(state->resultData), size, startRow, endRow);
                startRow = endRow;
            }
        }
        for (auto& t : workers) { if (t.joinable()) t.join(); }

        state->processingDone = true; // Mark as done *only* on success
        state->errorOccurred = false;
        // std::cout << LOG_PREFIX << "[" << state->socket << "] Computation finished successfully." << std::endl;

    } catch (const std::exception& e) {
        std::cerr << LOG_PREFIX << "[" << state->socket << "] EXCEPTION during computation: " << e.what() << std::endl;
        state->errorOccurred = true;
        state->processingDone = false; // Indicate it didn't finish correctly
    } catch (...) {
        std::cerr << LOG_PREFIX << "[" << state->socket << "] UNKNOWN EXCEPTION during computation." << std::endl;
        state->errorOccurred = true;
        state->processingDone = false;
    }
    state->processingStarted = false; // Computation attempt is over (success or fail)
}
// ----------------------------

// --- Client Handler ---
void handle_client(SOCKET clientSocket) {
    char clientIpStr[INET_ADDRSTRLEN];
    sockaddr_in clientAddr;
    int clientAddrSize = sizeof(clientAddr);
    std::string clientId = "Socket " + std::to_string(clientSocket); // Default ID

    if (getpeername(clientSocket, (struct sockaddr*)&clientAddr, &clientAddrSize) == 0) {
        inet_ntop(AF_INET, &clientAddr.sin_addr, clientIpStr, INET_ADDRSTRLEN);
        clientId = std::string(clientIpStr) + ":" + std::to_string(ntohs(clientAddr.sin_port)) + " (" + std::to_string(clientSocket) + ")";
        std::cout << LOG_PREFIX << "Client connected: " << clientId << std::endl;
    } else {
        std::cout << LOG_PREFIX << "Client connected: " << clientId << " (getpeername failed: " << WSAGetLastError() << ")" << std::endl;
    }

    ClientState state;
    state.socket = clientSocket;
    bool keep_connection = true;

    try {
        while (keep_connection) {
            uint32_t command;
            // std::cout << LOG_PREFIX << "[" << clientId << "] Waiting for command..." << std::endl;
            if (!recv_uint32(clientSocket, command, "waiting for command")) {
                keep_connection = false; break; // Exit loop if recv fails
            }
            // std::cout << LOG_PREFIX << "[" << clientId << "] Received command: " << command << std::endl;

            switch (command) {
                case CMD_CONFIG_DATA: {
                    if (!recv_uint32(clientSocket, state.matrixSize, "recv matrix size") ||
                        !recv_uint32(clientSocket, state.numThreads, "recv num threads")) {
                        keep_connection = false; break;
                    }
                    if (state.matrixSize == 0 || state.matrixSize > 3000) { // Increased limit slightly
                        std::cerr << LOG_PREFIX << "[" << clientId << "] Invalid matrix size received: " << state.matrixSize << std::endl;
                        send_uint32(clientSocket, RESP_ERROR, "send invalid size error"); // Try to send error
                        keep_connection = false; break; // Terminate connection on bad config
                    }
                    size_t dataSize = (size_t)state.matrixSize * state.matrixSize;
                    // std::cout << LOG_PREFIX << "[" << clientId << "] Receiving config: Size=" << state.matrixSize << ", Threads=" << state.numThreads << ", Elements=" << dataSize << std::endl;
                    if (!recv_floats(clientSocket, state.matrixData, dataSize, "recv matrix data")) {
                        keep_connection = false; break;
                    }
                    // Reset state for new data
                    state.dataReceived = true; state.processingStarted = false;
                    state.processingDone = false; state.errorOccurred = false;
                    if (state.workerThread.joinable()) state.workerThread.join(); // Clean up previous thread if any

                    if (!send_uint32(clientSocket, RESP_ACK, "send config ACK")) keep_connection = false;
                    break;
                }
                case CMD_START_COMP: {
                    if (!state.dataReceived) {
                        std::cerr << LOG_PREFIX << "[" << clientId << "] Error: START_COMP received before CONFIG_DATA." << std::endl;
                        if (!send_uint32(clientSocket, RESP_ERROR, "send start-before-config error")) keep_connection = false;
                        break; // Don't disconnect, just signal error for this command
                    }
                    if (state.processingStarted) { // Check atomic flag
                        std::cerr << LOG_PREFIX << "[" << clientId << "] Warning: START_COMP received while already processing." << std::endl;
                        // Send ACK, but don't restart computation
                        if (!send_uint32(clientSocket, RESP_ACK, "send duplicate start ACK")) keep_connection = false;
                        break;
                    }
                    // Join previous thread just in case (should be done, but safety)
                    if (state.workerThread.joinable()) state.workerThread.join();

                    // Set flags *before* starting thread
                    state.processingStarted = true; state.processingDone = false; state.errorOccurred = false;
                    // std::cout << LOG_PREFIX << "[" << clientId << "] Starting computation thread..." << std::endl;
                    state.workerThread = std::thread(perform_computation, &state); // Pass pointer to state

                    if (!send_uint32(clientSocket, RESP_ACK, "send start ACK")) keep_connection = false;
                    break;
                }
                case CMD_GET_STATUS: {
                    uint32_t response_code;
                    bool send_data = false;

                    if (state.errorOccurred)       response_code = RESP_ERROR;
                    else if (state.processingDone) { response_code = RESP_RESULT; send_data = true; }
                    else if (state.processingStarted) response_code = RESP_STATUS_PENDING;
                    else                           response_code = RESP_ERROR; // Error if no data/not started

                    if (!send_uint32(clientSocket, response_code, "send status response")) {
                        keep_connection = false; break;
                    }
                    if (send_data) {
                        // std::cout << LOG_PREFIX << "[" << clientId << "] Sending RESULT data (Size="<< state.matrixSize <<")..." << std::endl;
                        if (!send_uint32(clientSocket, state.matrixSize, "send result size") ||
                            !send_floats(clientSocket, state.resultData, "send result data")) {
                            keep_connection = false; // Error during result sending
                        }

                    }
                    break;
                }
                default:
                    std::cerr << LOG_PREFIX << "[" << clientId << "] Received unknown command: " << command << std::endl;
                    if (!send_uint32(clientSocket, RESP_ERROR, "send unknown command error")) keep_connection = false;
                    // Don't disconnect on unknown command, just report error
                    break;
            } // end switch
        } // end while(keep_connection)

    } catch (const std::exception& e) {
        std::cerr << LOG_PREFIX << "[" << clientId << "] Exception in client handler: " << e.what() << std::endl;
    } catch (...) {
        std::cerr << LOG_PREFIX << "[" << clientId << "] Unknown exception in client handler." << std::endl;
    }

    // --- Cleanup for this client ---
    std::cout << LOG_PREFIX << "Disconnecting client: " << clientId << std::endl;
    // State object destructor will handle joining the thread if needed.
    shutdown(clientSocket, SD_BOTH); // Signal close intent
    closesocket(clientSocket);       // Close the socket
}
// ----------------------

// --- Main Server Logic ---
int main() {
    WSADATA wsaData;
    int iResult = WSAStartup(MAKEWORD(2, 2), &wsaData);
    if (iResult != 0) {
        std::cerr << LOG_PREFIX << "WSAStartup failed: " << iResult << std::endl; return 1;
    }

    SOCKET listenSocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (listenSocket == INVALID_SOCKET) {
        std::cerr << LOG_PREFIX << "Socket creation failed: " << GetWSAErrorString(WSAGetLastError()) << std::endl;
        WSACleanup(); return 1;
    }

    // Allow address reuse - helps prevent "address already in use" errors on quick restarts
    char optval = 1;
    setsockopt(listenSocket, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));


    sockaddr_in serverAddr;
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_addr.s_addr = INADDR_ANY;
    serverAddr.sin_port = htons(SERVER_PORT);

    iResult = bind(listenSocket, (struct sockaddr*)&serverAddr, sizeof(serverAddr));
    if (iResult == SOCKET_ERROR) {
        std::cerr << LOG_PREFIX << "Bind failed: " << GetWSAErrorString(WSAGetLastError()) << std::endl;
        closesocket(listenSocket); WSACleanup(); return 1;
    }

    iResult = listen(listenSocket, SOMAXCONN);
    if (iResult == SOCKET_ERROR) {
        std::cerr << LOG_PREFIX << "Listen failed: " << GetWSAErrorString(WSAGetLastError()) << std::endl;
        closesocket(listenSocket); WSACleanup(); return 1;
    }

    std::cout << LOG_PREFIX << "Server listening on port " << SERVER_PORT << "..." << std::endl;

    while (true) {
        sockaddr_in clientAddrInfo; // To get client info for logging
        int clientAddrSize = sizeof(clientAddrInfo);
        SOCKET clientSocket = accept(listenSocket, (struct sockaddr*)&clientAddrInfo, &clientAddrSize);

        if (clientSocket == INVALID_SOCKET) {
            int error_code = WSAGetLastError();
            // Handle common errors during shutdown or temporary issues
            if (error_code == WSAEINTR || error_code == WSAECONNABORTED || error_code == WSAEMFILE || error_code == WSAENOBUFS) {
                std::cerr << LOG_PREFIX << "Accept failed temporarily: " << GetWSAErrorString(error_code) << ". Continuing..." << std::endl;
                std::this_thread::sleep_for(std::chrono::milliseconds(100)); // Short pause
                continue; // Try again
            } else {
                std::cerr << LOG_PREFIX << "Accept failed permanently?: " << GetWSAErrorString(error_code) << ". Shutting down." << std::endl;
                break; // Exit loop on critical accept error
            }
        }
        // Create and detach thread for the new client
        try {
            std::thread clientThread(handle_client, clientSocket);
            clientThread.detach();
        } catch (const std::system_error& e) {
            std::cerr << LOG_PREFIX << "Failed to create thread for client: " << e.what() << ". Closing socket." << std::endl;
            closesocket(clientSocket); // Clean up the socket if thread creation failed
        }
    }

    std::cout << LOG_PREFIX << "Shutting down listener socket." << std::endl;
    closesocket(listenSocket);
    WSACleanup();
    std::cout << LOG_PREFIX << "Server shut down complete." << std::endl;
    return 0;
}
