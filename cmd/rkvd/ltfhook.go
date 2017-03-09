package main

import (
	"os"

	"github.com/Sirupsen/logrus"
)

var defaultFormater = &logrus.TextFormatter{DisableColors: true}

// LogToFileHook saves log entries to a file after applying the Formatter.
type LogToFileHook struct {
	Formatter logrus.Formatter
	LogFile   *os.File
}

// NewLogToFileHook creates a new hook for logrus that logs all entries to a
// file. It uses a logrus.TextFormatter with DisableColors set to true. So your
// log files will be clean even though you have colors enabled in the terminal
// output.
func NewLogToFileHook(file *os.File) logrus.Hook {
	return &LogToFileHook{
		LogFile:   file,
		Formatter: defaultFormater,
	}
}

// Fire implements logrus.Hook.
func (l *LogToFileHook) Fire(entry *logrus.Entry) error {
	b, err := l.Formatter.Format(entry)

	if err != nil {
		return err
	}

	_, err = l.LogFile.Write(b)
	return err
}

// Levels implements logrus.Hook.
func (l *LogToFileHook) Levels() []logrus.Level {
	return logrus.AllLevels
}
