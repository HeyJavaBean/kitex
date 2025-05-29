package http

import (
	"bytes"
	"io"
	"net"
	"net/http"
)

type httpResponseWriter struct {
	conn net.Conn
	resp http.Response
}

func newHTTPResponseWriter(conn net.Conn) *httpResponseWriter {
	return &httpResponseWriter{
		conn: conn,
		resp: http.Response{
			StatusCode: http.StatusOK,
			ProtoMajor: 1,
			ProtoMinor: 1,
			Header:     make(http.Header),
			Body:       nil,
		},
	}
}

// Header returns the header map
func (w *httpResponseWriter) Header() http.Header {
	return w.resp.Header
}

// WriteHeader writes the header, status code in this case
func (w *httpResponseWriter) WriteHeader(statusCode int) {
	w.resp.StatusCode = statusCode
}

// Write writes the response body
func (w *httpResponseWriter) Write(data []byte) (int, error) {
	w.resp.Body = io.NopCloser(bytes.NewReader(data))
	w.resp.ContentLength = int64(len(data))
	return len(data), nil
}

func (w *httpResponseWriter) flush() error {
	return w.resp.Write(w.conn)
}
