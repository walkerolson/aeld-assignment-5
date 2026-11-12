#define _POSIX_C_SOURCE 200809L

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <syslog.h>
#include <unistd.h>

#define SERVER_PORT 9000
#define DATA_FILE "/var/tmp/aesdsocketdata"
#define RECEIVE_BUFFER_SIZE 1024

static volatile sig_atomic_t termination_requested;

static void handle_termination(int signal_number)
{
    (void)signal_number;
    termination_requested = 1;
}

static int report_errno(const char *operation)
{
    syslog(LOG_ERR, "%s: %s", operation, strerror(errno));
    return -1;
}

static int install_signal_handlers(void)
{
    struct sigaction action = {
        .sa_handler = handle_termination,
    };

    if (sigemptyset(&action.sa_mask) == -1) {
        return report_errno("sigemptyset");
    }

    if (sigaction(SIGINT, &action, NULL) == -1) {
        return report_errno("sigaction SIGINT");
    }

    if (sigaction(SIGTERM, &action, NULL) == -1) {
        return report_errno("sigaction SIGTERM");
    }

    action.sa_handler = SIG_IGN;
    if (sigaction(SIGPIPE, &action, NULL) == -1) {
        return report_errno("sigaction SIGPIPE");
    }

    return 0;
}

static int send_all(int socket_fd, const char *buffer, size_t length)
{
    size_t sent = 0;

    while (sent < length) {
        ssize_t result = send(socket_fd, buffer + sent, length - sent,
                              MSG_NOSIGNAL);

        if (result < 0) {
            if (errno == EINTR && !termination_requested) {
                continue;
            }
            return report_errno("send");
        }

        if (result == 0) {
            errno = EPIPE;
            return report_errno("send");
        }

        sent += (size_t)result;
    }

    return 0;
}

static int append_packet(int data_fd, const char *packet, size_t length)
{
    size_t written = 0;

    while (written < length) {
        ssize_t result = write(data_fd, packet + written, length - written);

        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            return report_errno("write data file");
        }

        if (result == 0) {
            errno = EIO;
            return report_errno("write data file");
        }

        written += (size_t)result;
    }

    return 0;
}

static int send_data_file(int data_fd, int client_fd)
{
    char buffer[RECEIVE_BUFFER_SIZE];

    if (lseek(data_fd, 0, SEEK_SET) == (off_t)-1) {
        return report_errno("lseek data file");
    }

    for (;;) {
        ssize_t bytes_read = read(data_fd, buffer, sizeof(buffer));

        if (bytes_read < 0) {
            if (errno == EINTR && !termination_requested) {
                continue;
            }
            return report_errno("read data file");
        }

        if (bytes_read == 0) {
            return 0;
        }

        if (send_all(client_fd, buffer, (size_t)bytes_read) == -1) {
            return -1;
        }
    }
}

static int reserve_packet_space(char **packet, size_t *capacity,
                                size_t required)
{
    size_t new_capacity = *capacity == 0 ? 256 : *capacity;
    char *new_packet;

    while (new_capacity < required) {
        if (new_capacity > SIZE_MAX / 2) {
            new_capacity = required;
            break;
        }
        new_capacity *= 2;
    }

    new_packet = realloc(*packet, new_capacity);
    if (new_packet == NULL) {
        syslog(LOG_ERR, "malloc failed while buffering packet; discarding packet");
        return -1;
    }

    *packet = new_packet;
    *capacity = new_capacity;
    return 0;
}

static int handle_client(int client_fd, int data_fd)
{
    char receive_buffer[RECEIVE_BUFFER_SIZE];
    char *packet = NULL;
    size_t packet_length = 0;
    size_t packet_capacity = 0;
    bool discard_packet = false;
    int result = 0;

    while (!termination_requested) {
        ssize_t bytes_received = recv(client_fd, receive_buffer,
                                      sizeof(receive_buffer), 0);

        if (bytes_received < 0) {
            if (errno == EINTR && !termination_requested) {
                continue;
            }
            if (!termination_requested) {
                result = report_errno("recv");
            }
            break;
        }

        if (bytes_received == 0) {
            break;
        }

        {
            size_t offset = 0;
            size_t received_length = (size_t)bytes_received;

            while (offset < received_length) {
                char *newline = memchr(receive_buffer + offset, '\n',
                                       received_length - offset);
                size_t segment_length = newline == NULL
                    ? received_length - offset
                    : (size_t)(newline - (receive_buffer + offset)) + 1;
                bool packet_too_large =
                    packet_length > SIZE_MAX - segment_length - 1;

                if (!discard_packet) {
                    if (packet_too_large ||
                        reserve_packet_space(&packet, &packet_capacity,
                                             packet_length + segment_length + 1) == -1) {
                        if (packet_too_large) {
                            syslog(LOG_ERR, "packet too large; discarding packet");
                        }
                        free(packet);
                        packet = NULL;
                        packet_capacity = 0;
                        packet_length = 0;
                        discard_packet = true;
                    } else {
                        memcpy(packet + packet_length,
                               receive_buffer + offset, segment_length);
                        packet_length += segment_length;
                        packet[packet_length] = '\0';
                    }
                }

                offset += segment_length;

                if (newline != NULL) {
                    if (!discard_packet) {
                        if (append_packet(data_fd, packet, packet_length) == -1 ||
                            send_data_file(data_fd, client_fd) == -1) {
                            result = -1;
                            goto done;
                        }
                        packet_length = 0;
                        packet[0] = '\0';
                    }
                    discard_packet = false;
                }
            }
        }
    }

done:
    free(packet);
    return result;
}

static int close_descriptor(int *descriptor, const char *description)
{
    int result;

    if (*descriptor == -1) {
        return 0;
    }

    result = close(*descriptor);
    *descriptor = -1;
    if (result == -1) {
        return report_errno(description);
    }

    return 0;
}

static int daemonize(void)
{
    pid_t child_pid = fork();

    if (child_pid == -1) {
        return report_errno("fork");
    }

    if (child_pid > 0) {
        return 1;
    }

    if (setsid() == -1) {
        return report_errno("setsid");
    }

    if (chdir("/") == -1) {
        return report_errno("chdir");
    }

    umask(0);

    {
        int null_fd = open("/dev/null", O_RDWR);

        if (null_fd == -1) {
            return report_errno("open /dev/null");
        }

        for (int descriptor = STDIN_FILENO; descriptor <= STDERR_FILENO;
             descriptor++) {
            if (dup2(null_fd, descriptor) == -1) {
                int saved_errno = errno;

                if (null_fd > STDERR_FILENO) {
                    close(null_fd);
                }
                errno = saved_errno;
                return report_errno("dup2 /dev/null");
            }
        }

        if (null_fd > STDERR_FILENO && close(null_fd) == -1) {
            return report_errno("close /dev/null");
        }
    }

    return 0;
}

int main(int argc, char *argv[])
{
    int server_fd = -1;
    int data_fd = -1;
    int exit_status = -1;
    bool daemon_mode = false;
    bool graceful_shutdown = false;
    struct sockaddr_in server_address = {
        .sin_family = AF_INET,
        .sin_port = htons(SERVER_PORT),
        .sin_addr.s_addr = htonl(INADDR_ANY),
    };

    openlog("aesdsocket", LOG_PID, LOG_USER);

    if (argc == 2 && strcmp(argv[1], "-d") == 0) {
        daemon_mode = true;
    } else if (argc != 1) {
        syslog(LOG_ERR, "Usage: aesdsocket [-d]");
        goto cleanup;
    }

    if (install_signal_handlers() == -1) {
        goto cleanup;
    }

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd == -1) {
        report_errno("socket");
        goto cleanup;
    }

    {
        int reuse_address = 1;

        if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse_address,
                       sizeof(reuse_address)) == -1) {
            report_errno("setsockopt");
            goto cleanup;
        }
    }

    if (bind(server_fd, (struct sockaddr *)&server_address,
             sizeof(server_address)) == -1) {
        report_errno("bind");
        goto cleanup;
    }

    if (daemon_mode) {
        int daemon_result = daemonize();

        if (daemon_result == -1) {
            goto cleanup;
        }

        if (daemon_result == 1) {
            int parent_exit_status =
                close_descriptor(&server_fd, "close parent server socket");

            closelog();
            return parent_exit_status;
        }
    }

    if (listen(server_fd, SOMAXCONN) == -1) {
        report_errno("listen");
        goto cleanup;
    }

    data_fd = open(DATA_FILE, O_CREAT | O_RDWR | O_APPEND, 0644);
    if (data_fd == -1) {
        report_errno("open data file");
        goto cleanup;
    }

    exit_status = 0;
    while (!termination_requested) {
        struct sockaddr_in client_address;
        socklen_t client_address_length = sizeof(client_address);
        char client_ip[INET_ADDRSTRLEN];
        int client_fd = accept(server_fd, (struct sockaddr *)&client_address,
                               &client_address_length);

        if (client_fd == -1) {
            if (errno == EINTR && termination_requested) {
                break;
            }
            if (errno == EINTR) {
                continue;
            }
            report_errno("accept");
            exit_status = -1;
            break;
        }

        if (inet_ntop(AF_INET, &client_address.sin_addr, client_ip,
                      sizeof(client_ip)) == NULL) {
            report_errno("inet_ntop");
            if (close(client_fd) == -1) {
                report_errno("close client socket");
            }
            exit_status = -1;
            break;
        }

        syslog(LOG_INFO, "Accepted connection from %s", client_ip);

        if (handle_client(client_fd, data_fd) == -1 &&
            !termination_requested) {
            exit_status = -1;
        }

        syslog(LOG_INFO, "Closed connection from %s", client_ip);
        if (close(client_fd) == -1) {
            report_errno("close client socket");
            exit_status = -1;
        }

        if (exit_status == -1) {
            break;
        }
    }

    graceful_shutdown = termination_requested != 0;
    if (graceful_shutdown) {
        syslog(LOG_INFO, "Caught signal, exiting");
    }

cleanup:
    if (termination_requested && !graceful_shutdown) {
        syslog(LOG_INFO, "Caught signal, exiting");
        graceful_shutdown = true;
    }

    if (close_descriptor(&data_fd, "close data file") == -1) {
        exit_status = -1;
    }
    if (close_descriptor(&server_fd, "close server socket") == -1) {
        exit_status = -1;
    }
    if (graceful_shutdown && unlink(DATA_FILE) == -1) {
        report_errno("unlink data file");
        exit_status = -1;
    }

    closelog();
    return exit_status;
}
