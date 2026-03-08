package utils

import "github.com/a-kazakov/gomr/parameters"

func StrOrDefault(str string, defaultValue string) string {
	if str == "" {
		return defaultValue
	}
	return str
}

func OptStrOrDefault(opt parameters.Optional[string], defaultValue string) string {
	if !opt.IsSet() {
		return defaultValue
	}
	v := opt.Get()
	if v == "" {
		return defaultValue
	}
	return v
}
