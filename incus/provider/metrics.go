package provider

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Sample is one series of /1.0/metrics as read.
type Sample struct {
	Name   string
	Labels map[string]string
	Value  float64
}

// ParseMetrics reads the text format Incus serves on /1.0/metrics: one `name{label="value",...} value [timestamp]` per
// line, and "#" lines (HELP, TYPE, EOF) that carry no sample. A line it cannot read fails the whole text: sums over a
// half-read text would be wrong without saying so.
func ParseMetrics(text string) ([]Sample, error) {
	samples := []Sample{}
	for index, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		isNotASample := line == "" || strings.HasPrefix(line, "#")
		if isNotASample {
			continue
		}
		sample, err := parseSample(line)
		if err != nil {
			return nil, fmt.Errorf("metrics line %d: %w", index+1, err)
		}
		samples = append(samples, sample)
	}
	return samples, nil
}

func parseSample(line string) (Sample, error) {
	nameEnd := strings.IndexAny(line, "{ ")
	if nameEnd <= 0 {
		return Sample{}, errors.New("no value after the series name")
	}
	sample := Sample{Name: line[:nameEnd], Labels: map[string]string{}}
	rest := line[nameEnd:]
	if rest[0] == '{' {
		labels, afterLabels, err := parseLabels(rest[1:])
		if err != nil {
			return Sample{}, fmt.Errorf("%s: %w", sample.Name, err)
		}
		sample.Labels = labels
		rest = afterLabels
	}
	// A timestamp may follow the value; the reader stamps the whole read instead.
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return Sample{}, fmt.Errorf("%s: no value", sample.Name)
	}
	value, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return Sample{}, fmt.Errorf("%s: value %q is not a number", sample.Name, fields[0])
	}
	sample.Value = value
	return sample, nil
}

// parseLabels reads `label="value",...}` and returns the text after the closing brace. A value is read character by
// character: it may hold commas, braces and the escapes \", \\ and \n.
func parseLabels(text string) (map[string]string, string, error) {
	labels := map[string]string{}
	position := 0
	for {
		if position >= len(text) {
			return nil, "", errors.New("labels not closed")
		}
		if text[position] == '}' {
			return labels, text[position+1:], nil
		}
		equals := strings.IndexByte(text[position:], '=')
		if equals <= 0 {
			return nil, "", errors.New("a label without a name or a value")
		}
		label := text[position : position+equals]
		if strings.ContainsAny(label, "\" ,{}") {
			return nil, "", fmt.Errorf("label name %q", label)
		}
		position += equals + 1
		if position >= len(text) || text[position] != '"' {
			return nil, "", fmt.Errorf("label %s: value not quoted", label)
		}
		position++

		var value strings.Builder
		closed := false
		for position < len(text) && !closed {
			character := text[position]
			isEscape := character == '\\' && position+1 < len(text)
			switch {
			case isEscape:
				value.WriteString(unescape(text[position+1]))
				position += 2
			case character == '"':
				closed = true
				position++
			default:
				value.WriteByte(character)
				position++
			}
		}
		if !closed {
			return nil, "", fmt.Errorf("label %s: value not closed", label)
		}
		labels[label] = value.String()

		if position < len(text) && text[position] == ',' {
			position++
			continue
		}
		if position >= len(text) || text[position] != '}' {
			return nil, "", fmt.Errorf("label %s: no comma or closing brace after its value", label)
		}
	}
}

// unescape is what the format writes after a backslash; anything else keeps the backslash.
func unescape(escaped byte) string {
	switch escaped {
	case 'n':
		return "\n"
	case '\\', '"':
		return string(escaped)
	default:
		return "\\" + string(escaped)
	}
}
