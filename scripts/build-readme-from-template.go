package main

import (
	"log"
	"os"
	"text/template"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatalf(
			"USAGE: go run scripts/build-readme-from-template.go <PATH_TO_TEMPLATE>",
		)
	}

	templateFilepath := os.Args[1]

	data, err := os.ReadFile(templateFilepath)
	if err != nil {
		log.Fatalf("unable read template '%s': %s", templateFilepath, err)
	}

	t, err := template.New(templateFilepath).Funcs(template.FuncMap{
		"readFile": readFile,
	}).Parse(string(data))
	if err != nil {
		log.Fatalf("unable to parse README template '%s': %s", templateFilepath, err)
	}

	f, err := os.OpenFile("README.md", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("unable to open README.md for writing: %s", err)
	}

	err = t.Execute(f, nil)
	if err != nil {
		log.Fatalf("error executing template file: %s", err)
	}
}

func readFile(path string) string {
	contents, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("error reading file '%s': %s", path, err)
	}

	return string(contents)
}
