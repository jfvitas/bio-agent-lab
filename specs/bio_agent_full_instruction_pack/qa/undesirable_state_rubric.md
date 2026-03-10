
# Undesirable State Detection Rubric

Testing agents must flag the following states as failures.

## Functional Failures

- required fields missing
- incorrect entity classification
- silently dropped records
- invalid schema outputs

## Scientific Failures

- misleading certainty when evidence weak
- incorrect binding interpretation
- pathway reasoning based on incomplete data

## UX Failures

- confusing CLI outputs
- unclear error messages
- results not discoverable

## Engineering Failures

- dead code
- duplicated logic
- hidden assumptions
- brittle scripts

Each issue must be reported with:

severity
location
description
suggested_fix
