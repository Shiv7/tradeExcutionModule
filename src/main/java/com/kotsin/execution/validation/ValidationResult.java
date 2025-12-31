package com.kotsin.execution.validation;

import lombok.Getter;
import java.util.ArrayList;
import java.util.List;

/**
 * Result object for validation operations
 */
@Getter
public class ValidationResult {
    private final List<String> errors = new ArrayList<>();
    private final List<String> warnings = new ArrayList<>();

    public void addError(String error) {
        errors.add(error);
    }

    public void addWarning(String warning) {
        warnings.add(warning);
    }

    public boolean isValid() {
        return errors.isEmpty();
    }

    public boolean hasWarnings() {
        return !warnings.isEmpty();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (!errors.isEmpty()) {
            sb.append("Errors: ").append(errors).append("\n");
        }
        if (!warnings.isEmpty()) {
            sb.append("Warnings: ").append(warnings);
        }
        return sb.toString();
    }
}
