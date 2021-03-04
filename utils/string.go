package utils

import "strings"

func TrimmedSplit(str string, sep string) []string {
	parts := strings.Split(str, sep)
	trimmedParts := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmedPart := strings.TrimSpace(part); trimmedPart != "" {
			trimmedParts = append(trimmedParts, trimmedPart)
		}
	}
	return trimmedParts
}
