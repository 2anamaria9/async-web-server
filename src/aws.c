// SPDX-License-Identifier: BSD-3-Clause

#include <math.h>
#include <linux/limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/sendfile.h>
#include <sys/eventfd.h>
#include <libaio.h>
#include <errno.h>

#include "aws.h"
#include "http-parser/http_parser.h"
#include "utils/util.h"
#include "utils/debug.h"
#include "utils/sock_util.h"
#include "utils/w_epoll.h"

/* server socket file descriptor */
static int listenfd;

/* epoll file descriptor */
static int epollfd;

static io_context_t ctx;

static int aws_on_path_cb(http_parser *p, const char *buf, size_t len)
{
	struct connection *conn = (struct connection *)p->data;

	memcpy(conn->request_path, buf, len);
	conn->request_path[len] = '\0';
	conn->have_path = 1;

	return 0;
}

static void connection_prepare_send_reply_header(struct connection *conn)
{
	/* TODO: Prepare the connection buffer to send the reply header. */
	conn->send_len = snprintf(conn->send_buffer, BUFSIZ,
		"HTTP/1.1 200 OK\r\n"
		"Content-Length: %lu\r\n"
		"Connection: close\r\n\r\n",
		conn->file_size);
	conn->send_pos = 0;
	conn->state = STATE_SENDING_HEADER;
	struct epoll_event ev;
	ev.events = EPOLLIN | EPOLLOUT;
	ev.data.ptr = conn;
	DIE(epoll_ctl(epollfd, EPOLL_CTL_MOD, conn->sockfd, &ev) < 0, "epoll_ctl EPOLL_CTL_MOD");
}

static void connection_prepare_send_404(struct connection *conn)
{
	/* TODO: Prepare the connection buffer to send the 404 header. */
	conn->send_len = snprintf(conn->send_buffer, BUFSIZ,
		"HTTP/1.1 404 Not Found\r\n"
		"Content-Length: 0\r\n"
		"Connection: close\r\n\r\n");
	conn->send_pos = 0;
	conn->state = STATE_SENDING_404;
	struct epoll_event ev;
	ev.events = EPOLLIN | EPOLLOUT;
	ev.data.ptr = conn;
	DIE(epoll_ctl(epollfd, EPOLL_CTL_MOD, conn->sockfd, &ev) < 0, "epoll_ctl EPOLL_CTL_MOD");
}

static enum resource_type connection_get_resource_type(struct connection *conn)
{
	/* TODO: Get resource type depending on request path/filename. Filename should
	 * point to the static or dynamic folder.
	 */
	if (strncmp(conn->request_path, "/static/", 8) == 0)
		return RESOURCE_TYPE_STATIC;
	if (strncmp(conn->request_path, "/dynamic/", 9) == 0)
		return RESOURCE_TYPE_DYNAMIC;
	return RESOURCE_TYPE_NONE;
	
}


struct connection *connection_create(int sockfd)
{
	/* TODO: Initialize connection structure on given socket. */
	struct connection *c = calloc(1, sizeof(*c));
	c->fd = -1;
	c->sockfd = sockfd;
	c->state = STATE_INITIAL;
	return c;
}

void connection_start_async_io(struct connection *conn)
{
	/* TODO: Start asynchronous operation (read from file).
	 * Use io_submit(2) & friends for reading data asynchronously.
	 */
	conn->eventfd = eventfd(0, 0);
	io_prep_pread(&conn->iocb, conn->fd, conn->send_buffer, BUFSIZ, conn->file_pos);
	conn->iocb.data = conn;
	conn->piocb[0] = &conn->iocb;
	io_submit(ctx, 1, conn->piocb);
	conn->state = STATE_ASYNC_ONGOING;
}

void connection_remove(struct connection *conn)
{
	/* TODO: Remove connection handler. */
	epoll_ctl(epollfd, EPOLL_CTL_DEL, conn->sockfd, NULL);
	if (conn->fd > -1)
		close(conn->fd);
	if (conn->eventfd > 0)
		close(conn->eventfd);
	close(conn->sockfd);
	free(conn);
}

void handle_new_connection(void)
{
	/* TODO: Handle a new connection request on the server socket. */

	/* TODO: Accept new connection. */
	int newsock = accept(listenfd, NULL, NULL);

	/* TODO: Set socket to be non-blocking. */
	int flags = fcntl(newsock, F_GETFL, 0);

	flags |= O_NONBLOCK;
	DIE(fcntl(newsock, F_SETFL, flags) < 0, "fcntl F_SETFL");

	/* TODO: Instantiate new connection handler. */
	struct connection *conn = connection_create(newsock);

	/* TODO: Add socket to epoll. */
	struct epoll_event ev;

	ev.events = EPOLLIN;
	ev.data.ptr = conn;

	epoll_ctl(epollfd, EPOLL_CTL_ADD, newsock, &ev);

	/* TODO: Initialize HTTP_REQUEST parser. */
	http_parser_init(&conn->request_parser, HTTP_REQUEST);
	conn->request_parser.data = conn;
}

void receive_data(struct connection *conn)
{
	/* TODO: Receive message on socket.
	 * Store message in recv_buffer in struct connection.
	 */
	ssize_t rc;

	while (1) {
		rc = recv(conn->sockfd, conn->recv_buffer + conn->recv_len, BUFSIZ - conn->recv_len, 0);
		if (rc > 0) {
			conn->recv_len = conn->recv_len + rc;
			continue;
		}
		if (rc == 0) {
			conn->state = STATE_CONNECTION_CLOSED;
			return;
		}
		if (errno == EAGAIN || errno == EWOULDBLOCK)
			break;
		conn->state = STATE_CONNECTION_CLOSED;
		return;
	}
	conn->state = STATE_RECEIVING_DATA;
}

int connection_open_file(struct connection *conn)
{
	/* TODO: Open file and update connection fields. */
	if (conn->res_type == RESOURCE_TYPE_STATIC)
		snprintf(conn->filename, BUFSIZ, "%s%s", AWS_ABS_STATIC_FOLDER, conn->request_path + 8);
	else if (conn->res_type == RESOURCE_TYPE_DYNAMIC)
		snprintf(conn->filename, BUFSIZ, "%s%s", AWS_ABS_DYNAMIC_FOLDER, conn->request_path + 9);
	
	conn->fd = open(conn->filename, O_RDONLY);
	if (conn->fd < 0)
		return -1;
	struct stat st;
	fstat(conn->fd, &st);
	conn->file_size = st.st_size;
	return 0;
}

void connection_complete_async_io(struct connection *conn)
{
	/* TODO: Complete asynchronous operation; operation returns successfully.
	 * Prepare socket for sending.
	 */
	struct io_event ev;
	io_getevents(ctx, 1, 1, &ev, NULL);
	conn->async_read_len = ev.res;
	conn->send_len = ev.res;
	conn->send_pos = 0;
	conn->file_pos = conn->file_pos + ev.res;
	conn->state = STATE_SENDING_DATA;
}

int parse_header(struct connection *conn)
{
	/* TODO: Parse the HTTP header and extract the file path. */
	/* Use mostly null settings except for on_path callback. */
	http_parser_settings settings = {0};
	settings.on_path = aws_on_path_cb;
	int n = http_parser_execute(&conn->request_parser, &settings, conn->recv_buffer, conn->recv_len);
	if (n != conn->recv_len)
		return -1;

	conn->res_type = connection_get_resource_type(conn);
	conn->state = STATE_REQUEST_RECEIVED;
	return 0;
}

enum connection_state connection_send_static(struct connection *conn)
{
	/* TODO: Send static data using sendfile(2). */
	while (conn->file_pos < conn->file_size) {
		off_t offset = conn->file_pos;
		ssize_t rc = sendfile(conn->sockfd, conn->fd, &offset, conn->file_size - conn->file_pos);
		if (rc > 0) {
			conn->file_pos = offset;
			continue;
		}
		if (rc == 0) {
			return STATE_DATA_SENT;
		}
		if (errno == EAGAIN || errno == EWOULDBLOCK)
				return STATE_SENDING_DATA;
		return STATE_CONNECTION_CLOSED;
	}
	return STATE_DATA_SENT;
}

int connection_send_data(struct connection *conn)
{
	/* May be used as a helper function. */
	/* TODO: Send as much data as possible from the connection send buffer.
	 * Returns the number of bytes sent or -1 if an error occurred
	 */
	ssize_t rc;
	while (conn->send_pos < conn->send_len) {
		rc = send(conn->sockfd, conn->send_buffer + conn->send_pos, conn->send_len - conn->send_pos, 0);
		if (rc > 0) {
			conn->send_pos = conn->send_pos + rc;
			continue;
		}
		if (rc < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
			return 0;
		}
		return -1;
	}
	return conn->send_pos;
}


int connection_send_dynamic(struct connection *conn)
{
	/* TODO: Read data asynchronously.
	 * Returns 0 on success and -1 on error.
	 */
	if (conn->send_pos < conn->send_len)
		return 0;
	if (conn->file_pos >= conn->file_size)
		return -1;
	connection_start_async_io(conn);
	return 0;
}


void handle_input(struct connection *conn)
{
	/* TODO: Handle input information: may be a new message or notification of
	 * completion of an asynchronous I/O operation.
	 */

	switch (conn->state) {
		case STATE_INITIAL:
		case STATE_RECEIVING_DATA:
			receive_data(conn);
			if (parse_header(conn) < 0 || conn->res_type == RESOURCE_TYPE_NONE || connection_open_file(conn) < 0) {
				connection_prepare_send_404(conn);
				break;
			}
			connection_prepare_send_reply_header(conn);
			break;
		case STATE_ASYNC_ONGOING:
			connection_complete_async_io(conn);
			break;
		default:
			break;
	}
}

void handle_output(struct connection *conn)
{
	/* TODO: Handle output information: may be a new valid requests or notification of
	 * completion of an asynchronous I/O operation or invalid requests.
	 */

	switch (conn->state) {
		case STATE_SENDING_HEADER:
		case STATE_SENDING_404:
			connection_send_data(conn);
			if (conn->send_pos == conn->send_len)
				conn->state = STATE_HEADER_SENT;
			break;
		case STATE_HEADER_SENT:
			if (conn->res_type == RESOURCE_TYPE_STATIC)
				conn->state = STATE_SENDING_DATA;
			else
				connection_start_async_io(conn);
			break;
		case STATE_SENDING_DATA:
			if (conn->res_type == RESOURCE_TYPE_STATIC)
				conn->state = connection_send_static(conn);
			else {
				if (connection_send_data(conn) < 0)
					conn->state = STATE_CONNECTION_CLOSED;
				else if (conn->send_pos == conn->send_len)
					connection_send_dynamic(conn);
			}
			break;
		case STATE_DATA_SENT:
			conn->state = STATE_CONNECTION_CLOSED;
			break;
		default:
			break;
	}
}

void handle_client(uint32_t event, struct connection *conn)
{
	/* TODO: Handle new client. There can be input and output connections.
	 * Take care of what happened at the end of a connection.
	 */
	if (event & EPOLLIN)
		handle_input(conn);
	if (event & EPOLLOUT)
		handle_output(conn);
	if (conn->state == STATE_CONNECTION_CLOSED)
		connection_remove(conn);
}

int main(void)
{

	/* TODO: Initialize asynchronous operations. */

	/* TODO: Initialize multiplexing. */

	/* TODO: Create server socket. */

	/* TODO: Add server socket to epoll object*/

	/* Uncomment the following line for debugging. */
	// dlog(LOG_INFO, "Server waiting for connections on port %d\n", AWS_LISTEN_PORT);

	io_setup(128, &ctx);

	epollfd = epoll_create1(0);

	listenfd = tcp_create_listener(AWS_LISTEN_PORT, 128);
	
	int flags = fcntl(listenfd, F_GETFL, 0);

	flags |= O_NONBLOCK;
	DIE(fcntl(listenfd, F_SETFL, flags) < 0, "fcntl F_SETFL");

	struct epoll_event ev;

	ev.events = EPOLLIN;
	ev.data.fd = listenfd;
	
	epoll_ctl(epollfd, EPOLL_CTL_ADD, listenfd, &ev);

	/* server main loop */
	struct epoll_event events[10];
	while (1) {
		/* TODO: Wait for events. */
		int n = epoll_wait(epollfd, events, 10, -1);
		/* TODO: Switch event types; consider
		 *   - new connection requests (on server socket)
		 *   - socket communication (on connection sockets)
		 */
		for (int i = 0; i < n; i++) {
			if (events[i].data.fd == listenfd)
				handle_new_connection();
			else
				handle_client(events[i].events, (struct connection *)events[i].data.ptr);
		}
	}

	return 0;
}
