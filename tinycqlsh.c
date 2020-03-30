// Minimalistic CQL client without any support for returned messages,
// can be used to bulk-load CQL data without other drivers/tools (and not much more).
// author: Piotr Sarna <sarna@scylladb.com>
// Use with rlwrap for shell-like history

#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <netinet/in.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>

static const char startup_msg[] = "\x04\x00\x00\x00\x01\x00\x00\x00\x16\x00\x01\x00\x0b\x43\x51"
        "\x4c\x5f\x56\x45\x52\x53\x49\x4f\x4e\x00\x05\x33\x2e\x30\x2e\x30";
static const char register_msg[] = "\x04\x00\x00\x01\x0b\x00\x00\x00\x31\x00\x03\x00\x0f\x54\x4f\x50\x4f\x4c\x4f\x47"
        "\x59\x5f\x43\x48\x41\x4e\x47\x45\x00\x0d\x53\x54\x41\x54\x55\x53\x5f\x43\x48"
        "\x41\x4e\x47\x45\x00\x0d\x53\x43\x48\x45\x4d\x41\x5f\x43\x48\x41\x4e\x47\x45";

static const char cql_header_for_query[] = "\x04\x00\x00\x03\x07";
static const char cql_footer[] = "\x00\x01\x14\x00\x00\x00\x64\x00\x08";

// Format of the query: [cql_header_for_query][packet_length][query_length][cql_footer]

void do_write(int fd, const char* buf, size_t size) {
    if (write(fd, buf, size) != size) {
        printf("Failed to write %lu bytes.", size);
        exit(1);
    }
}

void init_cql_connection(int sockfd) {
    do_write(sockfd, startup_msg, sizeof(startup_msg) - 1);
    do_write(sockfd, register_msg, sizeof(register_msg) - 1);
}

void send_cqls(int sockfd, FILE* source) {
    char *line = NULL;
    size_t n;
    ssize_t len = EOF;
    while((len = getline(&line, &n, source)) != EOF) {
        uint32_t network_query_len = htonl(len);
        uint32_t network_packet_len = htonl(len + 13);
        fprintf(stderr, "Sent> %s", line);
        do_write(sockfd, cql_header_for_query, sizeof(cql_header_for_query) - 1);
        do_write(sockfd, (const char*)&network_packet_len, sizeof(network_packet_len));
        do_write(sockfd, (const char*)&network_query_len, sizeof(network_query_len));
        do_write(sockfd, line, len);
        do_write(sockfd, cql_footer, sizeof(cql_footer) - 1);
    }
    free(line);
}

int main(int argc, char* argv[]) {
    int sockfd;
    struct sockaddr_in addr = {};

    if (argc != 3) {
        printf("Usage: %s HOST PORT\nTry %s 0 9042 for a local client.\n", argv[0], argv[0]);
        exit(1);
    }

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd == -1) {
        printf("Failed to create socket\n");
        exit(1);
    }

    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr(argv[1]);
    addr.sin_port = htons(atoi(argv[2]));

    if (connect(sockfd, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
        printf("Failed to connect\n");
        exit(1);
    }

    init_cql_connection(sockfd);
    send_cqls(sockfd, stdin);

    close(sockfd);
}

