package main

import (
	"log"
	"os"
	"text/template"
)

func main() {
	if len(os.Args) < 4 {
		log.Fatalf(
			"USAGE: go run scripts/build-readme-from-template.go PATH_TO_TEMPLATE PATH_TO_CRUD_EXAMPLE PATH_TO_BENCHMARK",
		)
	}

	templateFilepath := os.Args[1]
	crudExampleFilepath := os.Args[2]
	benchmarkFilepath := os.Args[3]

	data, err := os.ReadFile(templateFilepath)
	if err != nil {
		log.Fatalf("unable read template '%s': %s", templateFilepath, err)
	}

	t, err := template.New(templateFilepath).Parse(string(data))
	if err != nil {
		log.Fatalf("unable to parse README template '%s': %s", templateFilepath, err)
	}

	crudExample, err := os.ReadFile(crudExampleFilepath)
	if err != nil {
		log.Fatalf("unable to read benchmark results '%s': %s", benchmarkFilepath, err)
	}

	benchmark, err := os.ReadFile(benchmarkFilepath)
	if err != nil {
		log.Fatalf("unable to read benchmark results '%s': %s", benchmarkFilepath, err)
	}

	f, err := os.OpenFile("README.md", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("unable to open README.md for writing: %s", err)
	}

	err = t.Execute(f, map[string]interface{}{
		"crudExample": string(crudExample),
		"benchmark":   string(benchmark),
	})
	if err != nil {
		log.Fatalf("error executing template file: %s", err)
	}
}
