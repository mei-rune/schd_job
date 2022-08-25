package schd_job

import (
	"io"
	"os"
	"time"
)

func OpenFile(logfile, name string) (io.WriteCloser, error) {
	out, e := os.OpenFile(logfile, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if nil != e {
		logfile = ToFilename(logfile)

		out, e = os.OpenFile(logfile, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
		if nil != e {
			return nil, e
		}
	}
	io.WriteString(out, "=============== begin ( ")
	io.WriteString(out, time.Now().Format(time.RFC3339))
	io.WriteString(out, " ) ===============\r\n")

	return out, nil
}

func CloseFile(out io.WriteCloser) {
	defer out.Close()

	io.WriteString(out, "=============== end ( ")
	io.WriteString(out, time.Now().Format(time.RFC3339))
	io.WriteString(out, " ) ===============\r\n")
}
