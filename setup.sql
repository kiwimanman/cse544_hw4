CREATE TABLE RentalPlan (
    id              INTEGER PRIMARY KEY,
    name            VARCHAR(MAX) NOT NULL,
    maximum_rentals INTEGER NOT NULL,
    monthly_fee     MONEY NOT NULL
);

CREATE TABLE Customer (
    id         INTEGER PRIMARY KEY,
    login      VARCHAR(MAX) NOT NULL,
    password   VARCHAR(MAX) NOT NULL,
    first_name VARCHAR(MAX) NOT NULL,
    last_lname VARCHAR(MAX) NOT NULL,
    plan_id    INTEGER REFERENCES RentalPlan NOT NULL
);

CREATE TABLE Rental (
    customer_id   INTEGER NOT NULL REFERENCES Customer,
    movie_id      INTEGER NOT NULL,
    status        BIT NOT NULL,
    checkout_date DATE NOT NULL,
    checkout_time TIME NOT NULL
);

CREATE CLUSTERED INDEX Customer_Index       ON Customer(id);
CREATE CLUSTERED INDEX RentalPlan_Index     ON RentalPlan(id);
CREATE CLUSTERED INDEX Rental_Index         ON Rental(customer_id);

INSERT INTO RentalPlan (id, plan_name, maximum_rentals, monthly_fee)
VALUES (1, 'Basic Plan',                    1, 9.99);
       (2, 'Might As Well Plan',            2, 12.99),
       (3, 'Big ol Plan',                   5, 20.00),
       (4, 'We will not be open long plan', 100, 99.99);