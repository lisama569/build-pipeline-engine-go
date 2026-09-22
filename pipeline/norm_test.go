package pipeline

// stripMeasurements removes the only run-dependent measurement fields
// (duration_ms) from a JSON report so repeated runs can be compared for
// identical conclusions. Statuses, reasons, order, cache keys and summaries
// must survive normalization unchanged.
func stripMeasurements(raw []byte) []byte {
	out := make([]byte, 0, len(raw))
	s := string(raw)
	i := 0
	for i < len(s) {
		// find "duration_ms": number
		idx := indexFrom(s, "\"duration_ms\"", i)
		if idx < 0 {
			out = append(out, s[i:]...)
			break
		}
		out = append(out, s[i:idx]...)
		j := idx + len("\"duration_ms\"")
		for j < len(s) && s[j] != ':' {
			j++
		}
		j++ // skip ':'
		for j < len(s) && (s[j] == ' ' || s[j] == '\t') {
			j++
		}
		for j < len(s) && s[j] >= '0' && s[j] <= '9' {
			j++
		}
		out = append(out, "\"duration_ms\":0"...)
		i = j
	}
	return out
}

func indexFrom(s, sub string, from int) int {
	if from >= len(s) {
		return -1
	}
	for i := from; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
