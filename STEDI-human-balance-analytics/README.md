# STEDI Human Balance Analytics
## Implementation
### Landing Zone
SQL scripts:
- [customer_landing.sql](./scripts/customer_landing.sql)
- [accelerometer_landing.sql](./scripts/accelerometer_landing.sql)
- [step_trainer_landing.sql](./scripts/step_trainer_landing.sql)
### Trusted Zone
Job scripts:
- [customer_landing_to_trusted.py](./scripts/customer_landing_to_trusted.py)
- [accelerometer_landing_to_trusted.py](./scripts/accelerometer_landing_to_trusted.py)
- [step_trainer_landing_to_trusted.py](./scripts/step_trainer_landing_to_trusted.py)
### Curated Zone
Job scripts:
- [customer_trusted_to_curated.py](./scripts/customer_trusted_to_curated.py)
- [step_trainer_trusted_to_curated.py](./scripts/step_trainer_trusted_to_curated.py)
