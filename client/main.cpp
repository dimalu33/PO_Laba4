#include <winsock2.h>
#include <ws2tcpip.h>
#include <iostream>
#include <vector>
#include <random>
#include <chrono>
#include <thread>
#include <string>
#include <stdexcept>

#pragma comment(lib, "ws2_32.lib")

#define SERVER_IP "127.0.0.1"
#define SERVER_PORT 65001
#define LOG_PREFIX "[Client] "
#define DEFAULT_MATRIX_SIZE 5
#define DEFAULT_NUM_THREADS 2

const uint32_t CMD_CONFIG_DATA = 1;
const uint32_t CMD_START_COMP = 2;
const uint32_t CMD_GET_STATUS = 3;
const uint32_t RESP_ACK = 10;
const uint32_t RESP_STATUS_PENDING = 11;
const uint32_t RESP_RESULT = 12;
const uint32_t RESP_ERROR = 13;

std::string GetWSAErrorStringClient(int errorCode) {
    char* s = nullptr;
    FormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
                   NULL, errorCode, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), (LPSTR)&s, 0, NULL);
    std::string msg = (s ? s : "Unknown error");
    if(s) LocalFree(s);
    return msg + " (" + std::to_string(errorCode) + ")";
}

void send_uint32_or_throw(SOCKET sock, uint32_t value, const std::string& context) {
    uint32_t netValue = htonl(value);
    int bytesSent = send(sock, (const char*)&netValue, sizeof(netValue), 0);
    if (bytesSent == SOCKET_ERROR) {
        int error_code = WSAGetLastError();
        throw std::runtime_error(LOG_PREFIX + std::string("send_uint32 failed (") + context + "): " + GetWSAErrorStringClient(error_code));
    }
    if (bytesSent != sizeof(netValue)) {
        throw std::runtime_error(LOG_PREFIX + std::string("send_uint32 sent incomplete data (") + context + "): " + std::to_string(bytesSent) + "/" + std::to_string(sizeof(netValue)));
    }
}

uint32_t recv_uint32_or_throw(SOCKET sock, const std::string& context) {
    uint32_t value;
    int bytesReceived = recv(sock, (char*)&value, sizeof(value), MSG_WAITALL);
    if (bytesReceived == sizeof(value)) {
        return ntohl(value);
    }
    if (bytesReceived == 0) {
        throw std::runtime_error(LOG_PREFIX + std::string("recv_uint32 (") + context + "): Server disconnected gracefully.");
    } else if (bytesReceived == SOCKET_ERROR) {
        int error_code = WSAGetLastError();
        throw std::runtime_error(LOG_PREFIX + std::string("recv_uint32 failed (") + context + "): " + GetWSAErrorStringClient(error_code));
    } else {
        throw std::runtime_error(LOG_PREFIX + std::string("recv_uint32 received incomplete data (") + context + "): " + std::to_string(bytesReceived) + "/" + std::to_string(sizeof(value)));
    }
}

void send_floats_or_throw(SOCKET sock, const std::vector<float>& data, const std::string& context) {
    size_t totalBytes = data.size() * sizeof(float);
    if (totalBytes == 0) return;
    size_t bytesSent = 0;
    const char* buffer = reinterpret_cast<const char*>(data.data());
    while (bytesSent < totalBytes) {
        int result = send(sock, buffer + bytesSent, (int)(totalBytes - bytesSent), 0);
        if (result == SOCKET_ERROR) {
            int error_code = WSAGetLastError();
            throw std::runtime_error(LOG_PREFIX + std::string("send_floats failed (") + context + "): " + GetWSAErrorStringClient(error_code));
        }
        bytesSent += result;
    }
}

void recv_floats_or_throw(SOCKET sock, std::vector<float>& data, size_t count, const std::string& context) {
    if (count == 0) { data.clear(); return; }
    data.resize(count);
    size_t totalBytes = count * sizeof(float);
    size_t bytesReceived = 0;
    char* buffer = reinterpret_cast<char*>(data.data());
    while (bytesReceived < totalBytes) {
        int result = recv(sock, buffer + bytesReceived, (int)(totalBytes - bytesReceived), 0);
        if (result == SOCKET_ERROR) {
            int error_code = WSAGetLastError();
            throw std::runtime_error(LOG_PREFIX + std::string("recv_floats failed (") + context + "): " + GetWSAErrorStringClient(error_code));
        }
        if (result == 0) {
            throw std::runtime_error(LOG_PREFIX + std::string("recv_floats (") + context + "): Server disconnected before all data received ("
                                     + std::to_string(bytesReceived) + "/" + std::to_string(totalBytes) + ").");
        }
        bytesReceived += result;
    }
}

void generate_random_matrix(std::vector<float>& matrix, uint32_t size) {
    if (size == 0) { matrix.clear(); return; }
    matrix.resize((size_t)size * size);
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<float> distrib(0.0f, 100.0f);
    for (size_t i = 0; i < matrix.size(); ++i) matrix[i] = distrib(gen);
}

void print_matrix(const std::vector<float>& matrix, uint32_t size, const std::string& title) {
    std::cout << "\n--- " << title << " (Size: " << size << "x" << size << ") ---\n";
    if (size == 0 || matrix.empty()) { std::cout << "(Empty Matrix)\n"; return; }
    uint32_t print_limit = 10;
    for (uint32_t i = 0; i < std::min(size, print_limit); ++i) {
        for (uint32_t j = 0; j < std::min(size, print_limit); ++j) {
            size_t index = (size_t)i * size + j;
            if (index < matrix.size()) printf("%8.2f ", matrix[index]);
            else printf(" Error ");
        }
        if (size > print_limit) std::cout << "...";
        std::cout << std::endl;
    }
    if (size > print_limit) std::cout << "...\n";
    std::cout << "--------------------------------------\n";
}

int main(int argc, char* argv[]) {
    uint32_t matrixSize = DEFAULT_MATRIX_SIZE;
    uint32_t numThreads = DEFAULT_NUM_THREADS;

    if (argc > 1) try { matrixSize = std::stoul(argv[1]); } catch (...) { }
    if (argc > 2) try { numThreads = std::stoul(argv[2]); } catch (...) { }
    if (matrixSize == 0 || matrixSize > 5000) {
        std::cerr << LOG_PREFIX << "Warning: Invalid or large matrix size provided (" << matrixSize <<"), using default " << DEFAULT_MATRIX_SIZE << std::endl;
        matrixSize = DEFAULT_MATRIX_SIZE;
    }
    if (numThreads == 0 || numThreads > 128) {
        std::cerr << LOG_PREFIX << "Warning: Invalid thread count provided (" << numThreads <<"), using default " << DEFAULT_NUM_THREADS << std::endl;
        numThreads = DEFAULT_NUM_THREADS;
    }

    SOCKET connectSocket = INVALID_SOCKET;
    int exitCode = 0;
    WSADATA wsaData;
    bool wsaStarted = false;

    try {
        int iResult = WSAStartup(MAKEWORD(2, 2), &wsaData);
        if (iResult != 0) {
            throw std::runtime_error(LOG_PREFIX "WSAStartup failed: " + std::to_string(iResult));
        }
        wsaStarted = true;

        connectSocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (connectSocket == INVALID_SOCKET) {
            throw std::runtime_error(LOG_PREFIX "Socket creation failed: " + GetWSAErrorStringClient(WSAGetLastError()));
        }

        sockaddr_in serverAddr;
        serverAddr.sin_family = AF_INET;
        serverAddr.sin_port = htons(SERVER_PORT);
        if (inet_pton(AF_INET, SERVER_IP, &serverAddr.sin_addr) <= 0) {
            closesocket(connectSocket);
            throw std::runtime_error(LOG_PREFIX "Invalid server IP address format");
        }

        std::cout << LOG_PREFIX << "Connecting to server " << SERVER_IP << ":" << SERVER_PORT << "..." << std::endl;
        if (connect(connectSocket, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) == SOCKET_ERROR) {
            int error_code = WSAGetLastError();
            closesocket(connectSocket);
            throw std::runtime_error(LOG_PREFIX "Connect failed: " + GetWSAErrorStringClient(error_code));
        }
        std::cout << LOG_PREFIX << "Connected." << std::endl;

        std::vector<float> originalMatrix;
        generate_random_matrix(originalMatrix, matrixSize);
        print_matrix(originalMatrix, matrixSize, "Original Matrix (Client)");

        std::cout << LOG_PREFIX << "Sending configuration (Size=" << matrixSize << ", Threads=" << numThreads << ")..." << std::endl;
        send_uint32_or_throw(connectSocket, CMD_CONFIG_DATA, "send command config");
        send_uint32_or_throw(connectSocket, matrixSize, "send matrix size");
        send_uint32_or_throw(connectSocket, numThreads, "send num threads");
        send_floats_or_throw(connectSocket, originalMatrix, "send matrix data");

        uint32_t response = recv_uint32_or_throw(connectSocket, "recv config ack");
        if (response != RESP_ACK) throw std::runtime_error(LOG_PREFIX "Server did not ACK config. Response: " + std::to_string(response));
        std::cout << LOG_PREFIX << "Server acknowledged config." << std::endl;

        std::cout << LOG_PREFIX << "Sending start command..." << std::endl;
        send_uint32_or_throw(connectSocket, CMD_START_COMP, "send command start");
        response = recv_uint32_or_throw(connectSocket, "recv start ack");
        if (response != RESP_ACK) throw std::runtime_error(LOG_PREFIX "Server did not ACK start. Response: " + std::to_string(response));
        std::cout << LOG_PREFIX << "Server acknowledged start." << std::endl;

        std::cout << LOG_PREFIX << "Waiting for result (polling server)..." << std::endl;
        std::vector<float> resultMatrix;
        uint32_t resultSize = 0;
        bool result_received = false;
        int poll_attempts = 0;
        const int max_poll_attempts = 120;
        const int poll_interval_ms = 500;

        while (!result_received && poll_attempts < max_poll_attempts) {
            poll_attempts++;
            send_uint32_or_throw(connectSocket, CMD_GET_STATUS, "send command status");
            response = recv_uint32_or_throw(connectSocket, "recv status response");

            if (response == RESP_RESULT) {
                std::cout << LOG_PREFIX << "Status: Result received!" << std::endl;
                resultSize = recv_uint32_or_throw(connectSocket, "recv result size");
                if (resultSize != matrixSize) {
                    std::cerr << LOG_PREFIX << "Warning: Result matrix size (" << resultSize
                              << ") differs from original (" << matrixSize << ")" << std::endl;
                }
                if (resultSize > 0 && (size_t)resultSize * resultSize <= 100000000) {
                    recv_floats_or_throw(connectSocket, resultMatrix, (size_t)resultSize * resultSize, "recv result data");
                    print_matrix(resultMatrix, resultSize, "Result Matrix (Server)");
                } else if (resultSize == 0) {
                    std::cout << LOG_PREFIX << "Received empty result matrix (0x0)." << std::endl;
                    resultMatrix.clear();
                } else {
                    throw std::runtime_error(LOG_PREFIX "Received implausible result matrix size: " + std::to_string(resultSize));
                }
                result_received = true;

            } else if (response == RESP_STATUS_PENDING) {
                std::this_thread::sleep_for(std::chrono::milliseconds(poll_interval_ms));
            } else if (response == RESP_ERROR) {
                throw std::runtime_error(LOG_PREFIX "Server reported an error during processing.");
            } else {
                throw std::runtime_error(LOG_PREFIX "Received unexpected status response: " + std::to_string(response));
            }
        }

        if (!result_received) {
            std::cerr << LOG_PREFIX << "Error: Did not receive result after " << max_poll_attempts << " attempts." << std::endl;
            exitCode = 1;
        }

    } catch (const std::exception& e) {
        std::cerr << LOG_PREFIX << "Error: " << e.what() << std::endl;
        exitCode = 1;
    } catch (...) {
        std::cerr << LOG_PREFIX << "Error: An unknown exception occurred." << std::endl;
        exitCode = 1;
    }

    if (connectSocket != INVALID_SOCKET) {
        std::cout << LOG_PREFIX << "Closing connection." << std::endl;
        shutdown(connectSocket, SD_BOTH);
        closesocket(connectSocket);
    }
    if (wsaStarted) {
        WSACleanup();
    }
    std::cout << LOG_PREFIX << "Exiting." << (exitCode == 0 ? " Success." : " With errors.") << std::endl;
    return exitCode;
}