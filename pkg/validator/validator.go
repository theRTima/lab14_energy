package validator

/*
#cgo LDFLAGS: -L${SRCDIR}/../../rust/target/release -lmeter_validator
#include <stdint.h>
#include <stdlib.h>

typedef struct {
    double kwh;
    double voltage;
    double current;
} MeterReading;

typedef struct {
    int is_valid;
    int32_t error_code;
} ValidationResult;

ValidationResult validate_reading(const MeterReading* reading);
int validate_batch(const MeterReading* readings, size_t count, ValidationResult* results);
ValidationResult detect_anomaly(const MeterReading* current, const MeterReading* previous);
const char* get_error_message(int32_t error_code);
*/
import "C"
import (
	"fmt"
	"unsafe"

	"github.com/theRTima/lab14_energy/pkg/meter"
)

type ValidationError struct {
	Code    int
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation error [%d]: %s", e.Code, e.Message)
}

func ValidateReading(reading meter.Reading) error {
	cReading := C.MeterReading{
		kwh:     C.double(reading.KWh),
		voltage: C.double(reading.Voltage),
		current: C.double(reading.Current),
	}

	result := C.validate_reading(&cReading)

	if result.is_valid == 0 {
		errMsg := C.GoString(C.get_error_message(result.error_code))
		return &ValidationError{
			Code:    int(result.error_code),
			Message: errMsg,
		}
	}

	return nil
}

func ValidateBatch(readings []meter.Reading) ([]error, error) {
	if len(readings) == 0 {
		return nil, fmt.Errorf("empty readings slice")
	}

	cReadings := make([]C.MeterReading, len(readings))
	for i, r := range readings {
		cReadings[i] = C.MeterReading{
			kwh:     C.double(r.KWh),
			voltage: C.double(r.Voltage),
			current: C.double(r.Current),
		}
	}

	cResults := make([]C.ValidationResult, len(readings))

	ret := C.validate_batch(
		(*C.MeterReading)(unsafe.Pointer(&cReadings[0])),
		C.size_t(len(readings)),
		(*C.ValidationResult)(unsafe.Pointer(&cResults[0])),
	)

	if ret != 0 {
		return nil, fmt.Errorf("batch validation failed")
	}

	errors := make([]error, len(readings))
	for i, result := range cResults {
		if result.is_valid == 0 {
			errMsg := C.GoString(C.get_error_message(result.error_code))
			errors[i] = &ValidationError{
				Code:    int(result.error_code),
				Message: errMsg,
			}
		}
	}

	return errors, nil
}

func DetectAnomaly(current, previous meter.Reading) error {
	cCurrent := C.MeterReading{
		kwh:     C.double(current.KWh),
		voltage: C.double(current.Voltage),
		current: C.double(current.Current),
	}

	cPrevious := C.MeterReading{
		kwh:     C.double(previous.KWh),
		voltage: C.double(previous.Voltage),
		current: C.double(previous.Current),
	}

	result := C.detect_anomaly(&cCurrent, &cPrevious)

	if result.is_valid == 0 {
		errMsg := C.GoString(C.get_error_message(result.error_code))
		return &ValidationError{
			Code:    int(result.error_code),
			Message: errMsg,
		}
	}

	return nil
}
