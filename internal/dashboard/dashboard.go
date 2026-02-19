// Package dashboard provides a simple embedded web dashboard
package dashboard

import (
	"embed"
	"encoding/json"
	"html/template"
	"io/fs"
	"net/http"
)

// StaticFiles embedded static content
//
//go:embed static
var StaticFiles embed.FS

// HTML templates
//
//go:embed templates/*.html
var Templates embed.FS

// getStatic returns the static subdirectory as an fs.FS
func getStatic() fs.FS {
	sub, _ := fs.Sub(StaticFiles, "static")
	return sub
}

// Handler returns the dashboard HTTP handler
func Handler(clusterInfo interface {
	GetClusterInfo() interface{}
	GetNodes() interface{}
}) http.Handler {
	mux := http.NewServeMux()

	// Serve static files from embedded filesystem
	staticHandler := http.FileServer(getStatic())
	mux.Handle("/static/", http.StripPrefix("/static/", staticHandler))

	// Serve index
	mux.HandleFunc("/", indexHandler)

	// Serve metrics dashboard
	mux.HandleFunc("/_dashboard/metrics", metricsHandler)

	// Serve cluster dashboard
	mux.HandleFunc("/_dashboard/cluster", clusterHandler(clusterInfo))

	// Serve S3 browser
	mux.HandleFunc("/_dashboard/browser", s3BrowserHandler)

	return mux
}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	tmpl, err := template.ParseFS(Templates, "templates/index.html")
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	data := struct {
		Title   string
		Version string
	}{
		Title:   "OpenEndpoint Dashboard",
		Version: "1.0.0",
	}

	tmpl.Execute(w, data)
}

func metricsHandler(w http.ResponseWriter, r *http.Request) {
	tmpl, err := template.ParseFS(Templates, "templates/metrics.html")
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	data := struct {
		Title   string
		Version string
	}{
		Title:   "OpenEndpoint Metrics",
		Version: "1.0.0",
	}

	tmpl.Execute(w, data)
}

func clusterHandler(clusterInfo interface {
	GetClusterInfo() interface{}
	GetNodes() interface{}
}) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Check if it's an API request
		if r.URL.Query().Get("format") == "json" || r.Header.Get("Accept") == "application/json" {
			w.Header().Set("Content-Type", "application/json")

			var nodes interface{}
			var replicationFactor int

			if clusterInfo != nil {
				nodes = clusterInfo.GetNodes()
				info := clusterInfo.GetClusterInfo()
				if info != nil {
					if ci, ok := info.(interface{ ReplicationFactor() int }); ok {
						replicationFactor = ci.ReplicationFactor()
					}
				}
			} else {
				nodes = []interface{}{}
			}

			response := map[string]interface{}{
				"replicationFactor": replicationFactor,
				"totalStorage":      int64(0),
				"nodes":            nodes,
			}

			json.NewEncoder(w).Encode(response)
			return
		}

		// Serve HTML
		tmpl, err := template.ParseFS(Templates, "templates/cluster.html")
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}

		data := struct {
			Title   string
			Version string
		}{
			Title:   "OpenEndpoint Cluster",
			Version: "1.0.0",
		}

		tmpl.Execute(w, data)
	}
}

func s3BrowserHandler(w http.ResponseWriter, r *http.Request) {
	tmpl, err := template.ParseFS(Templates, "templates/browser.html")
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	data := struct {
		Title   string
		Version string
	}{
		Title:   "OpenEndpoint S3 Browser",
		Version: "1.0.0",
	}

	tmpl.Execute(w, data)
}
