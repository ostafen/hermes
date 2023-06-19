package http

import (
	"encoding/json"
	"net/http"

	"github.com/go-playground/validator"
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
	var input service.CreateProjectionInput

	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&input); err != nil {
		writeError(w, err, http.StatusInternalServerError)
		return
	}

	v := validator.New()
	if err := v.Struct(input); err != nil {
		writeError(w, err, http.StatusBadRequest)
		return
	}

	err := c.svc.Create(r.Context(), input)
	if err != nil {
		writeError(w, err, http.StatusInternalServerError)
		return
	}
}

func (c *ProjectionsController) Get(w http.ResponseWriter, r *http.Request) {

}

func (c *ProjectionsController) Delete(w http.ResponseWriter, r *http.Request) {

}

func writeJSON(w http.ResponseWriter, body any) {
	w.Header().Add("content-type", "application/json")

	data, err := json.Marshal(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_, err = w.Write(data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func writeError(w http.ResponseWriter, err error, status int) {
	http.Error(w, err.Error(), http.StatusInternalServerError)
}
