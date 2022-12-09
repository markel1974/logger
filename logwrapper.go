package logger

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"time"
)

const (
	LogFatal   = -1
	LogError   = 0
	LogWarning = 1
	LogInfo    = 2
	LogDebug   = 3
)

const (
	logUnknownString = "unknown"
	logFatalString   = "fatal"
	logErrorString   = "error"
	logInfoString    = "info"
	logWarningString = "warning"
	logDebugString   = "debug"
)

const (
	LogTextFormat = 0
	LogJsonFormat = 1
)

const (
	RFC3339Milli = "2006-01-02T15:04:05.000Z07:00"
)

type logWrapper struct {
	CurrentLogSeverity int
	CurrentLogFormat   int
	StringifyEvent     bool
}

var lwInst = &logWrapper{
	CurrentLogSeverity: LogInfo,
	CurrentLogFormat:   LogTextFormat,
	StringifyEvent:     true,
}

//var AppName string

//SetLogSeverity sets global log wrapper severity
func SetLogSeverity(severity int) {
	lwInst.CurrentLogSeverity = severity
}

//SetLogFormat sets the log output format; use LogTextFormat or LogJsonFormat
func SetLogFormat(fmt int) {
	lwInst.CurrentLogFormat = fmt
}

func StringifyEvent(s bool) {
	lwInst.StringifyEvent = s
}

type jsonLogRow struct {
	Timestamp  string      `json:"timestamp"`
	FileName   string      `json:"file"`
	LineNumber int         `json:"line"`
	MethodName string      `json:"method"`
	Message    string      `json:"message"`
	Event      interface{} `json:"event,omitempty"`
	Severity   string      `json:"severity"`
}

//var defaultLocation, _ = time.LoadLocation("Europe/Rome")

func eventToEventId(id string, event interface{}) interface{} {
	c := make(map[string]interface{})
	c[id] = event
	return c
}

func severityToString(severity int) string {
	var out string
	switch severity {
	case LogFatal:
		out = logFatalString
	case LogError:
		out = logErrorString
	case LogInfo:
		out = logInfoString
	case LogWarning:
		out = logWarningString
	case LogDebug:
		out = logDebugString
	default:
		out = logUnknownString
	}

	return out
}

func Fatal(format string, a ...interface{}) {
	logWithSeverity(LogFatal, nil, format, a...)
	os.Exit(255)
}

func Error(format string, a ...interface{}) {
	logWithSeverity(LogError, nil, format, a...)
}

func Info(format string, a ...interface{}) {
	logWithSeverity(LogInfo, nil, format, a...)
}

func Warning(format string, a ...interface{}) {
	logWithSeverity(LogWarning, nil, format, a...)
}

func Debug(format string, a ...interface{}) {
	logWithSeverity(LogDebug, nil, format, a...)
}

func FatalEvenId(eventId string, event interface{}, format string, a ...interface{}) {
	logWithSeverity(LogFatal, eventToEventId(eventId, event), format, a...)
	os.Exit(255)
}

func ErrorEventId(eventId string, event interface{}, format string, a ...interface{}) {
	logWithSeverity(LogError, eventToEventId(eventId, event), format, a...)
}

func InfoEventId(eventId string, event interface{}, format string, a ...interface{}) {
	logWithSeverity(LogInfo, eventToEventId(eventId, event), format, a...)
}

func WarningEventId(eventId string, event interface{}, format string, a ...interface{}) {
	logWithSeverity(LogWarning, eventToEventId(eventId, event), format, a...)
}

func DebugEventId(eventId string, event interface{}, format string, a ...interface{}) {
	logWithSeverity(LogDebug, eventToEventId(eventId, event), format, a...)
}

func FatalEvent(event interface{}, format string, a ...interface{}) {
	logWithSeverity(LogFatal, event, format, a...)
	os.Exit(255)
}

func ErrorEvent(event interface{}, format string, a ...interface{}) {
	logWithSeverity(LogError, event, format, a...)
}

func InfoEvent(event interface{}, format string, a ...interface{}) {
	logWithSeverity(LogInfo, event, format, a...)
}

func WarningEvent(event interface{}, format string, a ...interface{}) {
	logWithSeverity(LogWarning, event, format, a...)
}

func DebugEvent(event interface{}, format string, a ...interface{}) {
	logWithSeverity(LogDebug, event, format, a...)
}

func logWithSeverity(severity int, event interface{}, format string, a ...interface{}) {
	timestamp := getTimestamp()
	lineNumber, fileName, methodName := getCallerStack()
	formattedString := fmt.Sprintf(format, a...)
	log.SetFlags(0)
	if severity <= lwInst.CurrentLogSeverity {
		if lwInst.CurrentLogFormat == LogTextFormat {
			wholeRow := fmt.Sprintf("[%v][%v][%v][%d] %v", timestamp, severityToString(severity), fileName, lineNumber, formattedString)
			log.Println(wholeRow)
		} else {
			jsonRow := new(jsonLogRow)
			jsonRow.Timestamp = timestamp
			jsonRow.FileName = fileName
			jsonRow.LineNumber = lineNumber
			jsonRow.MethodName = methodName
			if event != nil {
				if lwInst.StringifyEvent {
					if e, err := json.Marshal(event); err == nil {
						jsonRow.Event = string(e)
					}
				} else {
					jsonRow.Event = event
				}
			}
			jsonRow.Message = formattedString
			jsonRow.Severity = severityToString(severity)
			if bv, err := json.Marshal(jsonRow); err == nil {
				log.Println(string(bv))
			}
		}
	}
}

func getTimestamp() string {
	t := time.Now().Local()
	//t := time.Now().UTC()
	//if defaultLocation != nil {
	//	t = t.In(defaultLocation)
	//}
	return t.Format(RFC3339Milli)
}

func buildFilename(in string, sep string) (string, bool) {
	if fls := strings.Split(in, sep); len(fls) > 0 {
		if len(fls) > 1 {
			return fls[len(fls)-2] + string(os.PathSeparator) + fls[len(fls)-1], true
		}
	}
	return in, false
}

func getCallerStack() (int, string, string) {
	lineNumber := 0
	fileName := ""
	methodName := ""
	pc := make([]uintptr, 10)
	runtime.Callers(3, pc)

	idx := 1
	if f := runtime.FuncForPC(pc[idx]); f != nil {
		fileName, lineNumber = f.FileLine(pc[idx])
		//if pos := strings.LastIndex(fileName, AppName); pos >= 0 {
		//	fileName = fileName[pos:]
		//}
		var ok bool
		if fileName, ok = buildFilename(fileName, "/"); !ok {
			fileName, _ = buildFilename(fileName, "\\")
		}

		methodName = f.Name()
		if pos := strings.LastIndex(methodName, "/"); pos >= 0 {
			if len(methodName) > pos+1 {
				pos++
			}
			methodName = methodName[pos:]
		}
	}

	/*
		_, fileName, lineNumber, ok := runtime.Caller(3)
	    if ok {
			if pos := strings.LastIndex(fileName, AppName); pos >= 0 {
				fileName = fileName[pos:]
			}

	        //ret = fmt.Sprintf("%s:%d", fileName, lineNumber)
		}
	*/
	return lineNumber, fileName, methodName
}
