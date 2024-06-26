// Code generated by go-swagger; DO NOT EDIT.

package enterprises

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"fmt"
	"io"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"

	apiserver_params "github.com/cloudbase/garm/apiserver/params"
	garm_params "github.com/cloudbase/garm/params"
)

// UpdateEnterprisePoolReader is a Reader for the UpdateEnterprisePool structure.
type UpdateEnterprisePoolReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *UpdateEnterprisePoolReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewUpdateEnterprisePoolOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewUpdateEnterprisePoolDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewUpdateEnterprisePoolOK creates a UpdateEnterprisePoolOK with default headers values
func NewUpdateEnterprisePoolOK() *UpdateEnterprisePoolOK {
	return &UpdateEnterprisePoolOK{}
}

/*
UpdateEnterprisePoolOK describes a response with status code 200, with default header values.

Pool
*/
type UpdateEnterprisePoolOK struct {
	Payload garm_params.Pool
}

// IsSuccess returns true when this update enterprise pool o k response has a 2xx status code
func (o *UpdateEnterprisePoolOK) IsSuccess() bool {
	return true
}

// IsRedirect returns true when this update enterprise pool o k response has a 3xx status code
func (o *UpdateEnterprisePoolOK) IsRedirect() bool {
	return false
}

// IsClientError returns true when this update enterprise pool o k response has a 4xx status code
func (o *UpdateEnterprisePoolOK) IsClientError() bool {
	return false
}

// IsServerError returns true when this update enterprise pool o k response has a 5xx status code
func (o *UpdateEnterprisePoolOK) IsServerError() bool {
	return false
}

// IsCode returns true when this update enterprise pool o k response a status code equal to that given
func (o *UpdateEnterprisePoolOK) IsCode(code int) bool {
	return code == 200
}

// Code gets the status code for the update enterprise pool o k response
func (o *UpdateEnterprisePoolOK) Code() int {
	return 200
}

func (o *UpdateEnterprisePoolOK) Error() string {
	return fmt.Sprintf("[PUT /enterprises/{enterpriseID}/pools/{poolID}][%d] updateEnterprisePoolOK  %+v", 200, o.Payload)
}

func (o *UpdateEnterprisePoolOK) String() string {
	return fmt.Sprintf("[PUT /enterprises/{enterpriseID}/pools/{poolID}][%d] updateEnterprisePoolOK  %+v", 200, o.Payload)
}

func (o *UpdateEnterprisePoolOK) GetPayload() garm_params.Pool {
	return o.Payload
}

func (o *UpdateEnterprisePoolOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewUpdateEnterprisePoolDefault creates a UpdateEnterprisePoolDefault with default headers values
func NewUpdateEnterprisePoolDefault(code int) *UpdateEnterprisePoolDefault {
	return &UpdateEnterprisePoolDefault{
		_statusCode: code,
	}
}

/*
UpdateEnterprisePoolDefault describes a response with status code -1, with default header values.

APIErrorResponse
*/
type UpdateEnterprisePoolDefault struct {
	_statusCode int

	Payload apiserver_params.APIErrorResponse
}

// IsSuccess returns true when this update enterprise pool default response has a 2xx status code
func (o *UpdateEnterprisePoolDefault) IsSuccess() bool {
	return o._statusCode/100 == 2
}

// IsRedirect returns true when this update enterprise pool default response has a 3xx status code
func (o *UpdateEnterprisePoolDefault) IsRedirect() bool {
	return o._statusCode/100 == 3
}

// IsClientError returns true when this update enterprise pool default response has a 4xx status code
func (o *UpdateEnterprisePoolDefault) IsClientError() bool {
	return o._statusCode/100 == 4
}

// IsServerError returns true when this update enterprise pool default response has a 5xx status code
func (o *UpdateEnterprisePoolDefault) IsServerError() bool {
	return o._statusCode/100 == 5
}

// IsCode returns true when this update enterprise pool default response a status code equal to that given
func (o *UpdateEnterprisePoolDefault) IsCode(code int) bool {
	return o._statusCode == code
}

// Code gets the status code for the update enterprise pool default response
func (o *UpdateEnterprisePoolDefault) Code() int {
	return o._statusCode
}

func (o *UpdateEnterprisePoolDefault) Error() string {
	return fmt.Sprintf("[PUT /enterprises/{enterpriseID}/pools/{poolID}][%d] UpdateEnterprisePool default  %+v", o._statusCode, o.Payload)
}

func (o *UpdateEnterprisePoolDefault) String() string {
	return fmt.Sprintf("[PUT /enterprises/{enterpriseID}/pools/{poolID}][%d] UpdateEnterprisePool default  %+v", o._statusCode, o.Payload)
}

func (o *UpdateEnterprisePoolDefault) GetPayload() apiserver_params.APIErrorResponse {
	return o.Payload
}

func (o *UpdateEnterprisePoolDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}
