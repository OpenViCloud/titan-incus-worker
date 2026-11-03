package controlplane

import "net/url"

// The two outcome words of a request's report (titan-ops architecture/resource-lifecycle.md#callback-routes).
const (
	OutcomeDone   = "Done"
	OutcomeFailed = "Failed"
)

// MaxDetail is how many runes of the raw error a Failed sends (titan-ops architecture/resource-lifecycle.md#callback-routes).
const MaxDetail = 1000

// Detail is the raw error a Failed carries for the admin, cut at MaxDetail runes, never inside one.
func Detail(cause error) string {
	detail := []rune(cause.Error())
	if len(detail) > MaxDetail {
		detail = detail[:MaxDetail]
	}
	return string(detail)
}

// RequestPath is the route that ends the request a command carries, under the resource's own path
// ("/internal/volumes/{volumeId}", say).
func RequestPath(resourcePath string, requestId string) string {
	return resourcePath + "/requests/" + url.PathEscape(requestId)
}

// ObservationPath is the reconciler's route for one resource, under the resource's own path.
func ObservationPath(resourcePath string) string {
	return resourcePath + "/observation"
}
