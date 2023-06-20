package http

import (
	"io"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/ostafen/hermes/internal/service"
)

type ProjectionsController struct {
	svc service.ProjectionService
}

func NewProjectionsController(svc service.ProjectionService) *ProjectionsController {
	return &ProjectionsController{
		svc: svc,
	}
}

func (c *ProjectionsController) Create(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	query, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if len(query) == 0 {
		writeError(w, "query must not be empty", http.StatusBadRequest)
		return
	}

	err = c.svc.Create(r.Context(), service.CreateProjectionInput{
		Name:  vars["name"],
		Query: string(query),
	})

	if err != nil {
		writeError(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (c *ProjectionsController) Delete(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	err := c.svc.Delete(r.Context(), service.DeleteProjectionInput{
		Name: vars["name"],
	})
	if err != nil {
		writeError(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func writeError(w http.ResponseWriter, errMsg string, status int) {
	http.Error(w, errMsg, http.StatusInternalServerError)
}
