-- Keith Stone
-- CSE 544 Winter 2014
-- Homework #4

CREATE TABLE RentalPlan (
    id              INTEGER IDENTITY(1,1) PRIMARY KEY,
    name            VARCHAR(MAX) NOT NULL,
    maximum_rentals INTEGER NOT NULL,
    monthly_fee     MONEY NOT NULL
);

CREATE TABLE Customer (
      id         INTEGER IDENTITY(1,1) PRIMARY KEY,
      login      VARCHAR(MAX) NOT NULL,
      password   VARCHAR(MAX) NOT NULL,
      fname VARCHAR(MAX) NOT NULL,
      lname VARCHAR(MAX) NOT NULL,
      plan_id    INTEGER REFERENCES RentalPlan NOT NULL
  );

  CREATE TABLE Rental (
      id            INTEGER IDENTITY(1,1) PRIMARY KEY,
      customer_id   INTEGER NOT NULL REFERENCES Customer,
      movie_id      INTEGER NOT NULL,
      status        BIT NOT NULL DEFAULT(1),
      checkout      DATETIME NOT NULL DEFAULT GETDATE()
  );

CREATE CLUSTERED INDEX Rental_Index         ON Rental(customer_id);

INSERT INTO RentalPlan (name, maximum_rentals, monthly_fee)
VALUES ('Basic Plan',                    1, 9.99),
       ('Might As Well Plan',            2, 12.99),
       ('Big ol Plan',                   5, 20.00),
       ('We will not be open long plan', 100, 99.99);

INSERT INTO Customer (login, password, fname, lname, plan_id)
VALUES ('username',   'password', 'User',     'Name',  1),
       ('ruby',       'password', 'Yukihiro', 'Matz',  2),
       ('kiwimanman', 'password', 'Keith',    'Stone', 3),
       ('suki',       'password', 'My',       'Cat',   4);

INSERT INTO Rental (customer_id, movie_id)
VALUES (3, 255074);