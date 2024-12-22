# STEDI Human Balance Analytics

## Implementation

### Landing Zone
SQL scripts:
- [customer_landing.sql](./scripts/customer_landing.sql)
- [accelerometer_landing.sql](./scripts/accelerometer_landing.sql)
- [step_trainer_landing.sql](./scripts/step_trainer_landing.sql)

#### Customer Landing Query Result
![customer_landing](./pictures/customer_landing.jpg)

#### Customer Landing with shareWithResearchAsOfDate is null
![customer_landing_null](./pictures/customer_landing_null.jpg)

#### Accelerometer Landing Query Result
![accelerometer_landing](./pictures/accelerometer_landing.jpg)

#### Step Trainer Landing Query Result
![step_trainer_landing](./pictures/step_trainer_landing.jpg)

### Trusted Zone
Job scripts:
- [customer_landing_to_trusted.py](./scripts/customer_landing_to_trusted.py)
- [accelerometer_landing_to_trusted.py](./scripts/accelerometer_landing_to_trusted.py)
- [step_trainer_landing_to_trusted.py](./scripts/step_trainer_landing_to_trusted.py)

#### Customer Trusted Query Result
![customer_trusted](./pictures/customer_trusted.jpg)

#### Customer Landing with shareWithResearchAsOfDate is not null
![customer_trusted_not_null](./pictures/customer_trusted_not_null.jpg)

#### Accelerometer Trusted Query Result
![accelerometer_trusted](./pictures/accelerometer_trusted.jpg)

#### Step Trainer Trusted Query Result
![step_trainer_trusted](./pictures/step_trainer_trusted.jpg)

### Curated Zone
Job scripts:
- [customer_trusted_to_curated.py](./scripts/customer_trusted_to_curated.py)
- [step_trainer_trusted_to_curated.py](./scripts/step_trainer_trusted_to_curated.py)

#### Customer Curated Query Result
![customer_curated](./pictures/customer_curated.jpg)

#### Machine Learning Curated Query Result
![machine_learning_curated](./pictures/machine_learning_curated.jpg)
