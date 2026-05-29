use std::ffi::CStr;
use std::os::raw::c_char;

#[repr(C)]
pub struct MeterReading {
    pub kwh: f64,
    pub voltage: f64,
    pub current: f64,
}

#[repr(C)]
pub struct ValidationResult {
    pub is_valid: bool,
    pub error_code: i32,
}

const ERROR_NONE: i32 = 0;
const ERROR_KWH_OUT_OF_RANGE: i32 = 1;
const ERROR_VOLTAGE_OUT_OF_RANGE: i32 = 2;
const ERROR_CURRENT_OUT_OF_RANGE: i32 = 3;
const ERROR_POWER_MISMATCH: i32 = 4;
const ERROR_ANOMALY_DETECTED: i32 = 5;

const MIN_KWH: f64 = 0.0;
const MAX_KWH: f64 = 50.0;
const MIN_VOLTAGE: f64 = 200.0;
const MAX_VOLTAGE: f64 = 240.0;
const MIN_CURRENT: f64 = 0.0;
const MAX_CURRENT: f64 = 100.0;

#[no_mangle]
pub extern "C" fn validate_reading(reading: *const MeterReading) -> ValidationResult {
    if reading.is_null() {
        return ValidationResult {
            is_valid: false,
            error_code: -1,
        };
    }

    let reading = unsafe { &*reading };

    if reading.kwh < MIN_KWH || reading.kwh > MAX_KWH {
        return ValidationResult {
            is_valid: false,
            error_code: ERROR_KWH_OUT_OF_RANGE,
        };
    }

    if reading.voltage < MIN_VOLTAGE || reading.voltage > MAX_VOLTAGE {
        return ValidationResult {
            is_valid: false,
            error_code: ERROR_VOLTAGE_OUT_OF_RANGE,
        };
    }

    if reading.current < MIN_CURRENT || reading.current > MAX_CURRENT {
        return ValidationResult {
            is_valid: false,
            error_code: ERROR_CURRENT_OUT_OF_RANGE,
        };
    }

    let calculated_power = (reading.voltage * reading.current) / 1000.0;
    let power_diff = (calculated_power - reading.kwh).abs();
    let tolerance = reading.kwh * 0.15;

    if power_diff > tolerance {
        return ValidationResult {
            is_valid: false,
            error_code: ERROR_POWER_MISMATCH,
        };
    }

    ValidationResult {
        is_valid: true,
        error_code: ERROR_NONE,
    }
}

#[no_mangle]
pub extern "C" fn validate_batch(
    readings: *const MeterReading,
    count: usize,
    results: *mut ValidationResult,
) -> i32 {
    if readings.is_null() || results.is_null() || count == 0 {
        return -1;
    }

    let readings_slice = unsafe { std::slice::from_raw_parts(readings, count) };
    let results_slice = unsafe { std::slice::from_raw_parts_mut(results, count) };

    for (i, reading) in readings_slice.iter().enumerate() {
        results_slice[i] = validate_reading(reading as *const MeterReading);
    }

    0
}

#[no_mangle]
pub extern "C" fn detect_anomaly(
    current: *const MeterReading,
    previous: *const MeterReading,
) -> ValidationResult {
    if current.is_null() || previous.is_null() {
        return ValidationResult {
            is_valid: false,
            error_code: -1,
        };
    }

    let current = unsafe { &*current };
    let previous = unsafe { &*previous };

    let kwh_change = ((current.kwh - previous.kwh) / previous.kwh).abs();
    if kwh_change > 0.5 {
        return ValidationResult {
            is_valid: false,
            error_code: ERROR_ANOMALY_DETECTED,
        };
    }

    let voltage_change = ((current.voltage - previous.voltage) / previous.voltage).abs();
    if voltage_change > 0.1 {
        return ValidationResult {
            is_valid: false,
            error_code: ERROR_ANOMALY_DETECTED,
        };
    }

    ValidationResult {
        is_valid: true,
        error_code: ERROR_NONE,
    }
}

#[no_mangle]
pub extern "C" fn get_error_message(error_code: i32) -> *const c_char {
    let message = match error_code {
        ERROR_NONE => "No error\0",
        ERROR_KWH_OUT_OF_RANGE => "Power consumption out of valid range\0",
        ERROR_VOLTAGE_OUT_OF_RANGE => "Voltage out of valid range\0",
        ERROR_CURRENT_OUT_OF_RANGE => "Current out of valid range\0",
        ERROR_POWER_MISMATCH => "Power calculation mismatch\0",
        ERROR_ANOMALY_DETECTED => "Anomaly detected in readings\0",
        _ => "Unknown error\0",
    };

    message.as_ptr() as *const c_char
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_reading() {
        let reading = MeterReading {
            kwh: 5.0,
            voltage: 220.0,
            current: 22.73,
        };

        let result = validate_reading(&reading as *const MeterReading);
        assert!(result.is_valid);
        assert_eq!(result.error_code, ERROR_NONE);
    }

    #[test]
    fn test_kwh_out_of_range() {
        let reading = MeterReading {
            kwh: 100.0,
            voltage: 220.0,
            current: 10.0,
        };

        let result = validate_reading(&reading as *const MeterReading);
        assert!(!result.is_valid);
        assert_eq!(result.error_code, ERROR_KWH_OUT_OF_RANGE);
    }

    #[test]
    fn test_voltage_out_of_range() {
        let reading = MeterReading {
            kwh: 5.0,
            voltage: 300.0,
            current: 10.0,
        };

        let result = validate_reading(&reading as *const MeterReading);
        assert!(!result.is_valid);
        assert_eq!(result.error_code, ERROR_VOLTAGE_OUT_OF_RANGE);
    }

    #[test]
    fn test_anomaly_detection() {
        let previous = MeterReading {
            kwh: 5.0,
            voltage: 220.0,
            current: 22.73,
        };

        let current = MeterReading {
            kwh: 10.0,
            voltage: 220.0,
            current: 45.45,
        };

        let result = detect_anomaly(&current as *const MeterReading, &previous as *const MeterReading);
        assert!(!result.is_valid);
        assert_eq!(result.error_code, ERROR_ANOMALY_DETECTED);
    }
}
