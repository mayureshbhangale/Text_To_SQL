"""
Create representative CoSQL/Spider SQLite databases for integration testing.

Schemas match the published Spider benchmark databases exactly:
- concert_singer: stadiums, singers, concerts
- pets_1:         students and their pets
- car_1:          car makers, models, stats

Run: python scripts/create_cosql_fixtures.py
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

DB_DIR = Path(__file__).parent.parent / "data" / "cosql" / "databases"
DB_DIR.mkdir(parents=True, exist_ok=True)


# ── concert_singer ────────────────────────────────────────────────────────────

def create_concert_singer() -> None:
    db_path = DB_DIR / "concert_singer" / "concert_singer.db"
    db_path.parent.mkdir(exist_ok=True)

    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.executescript("""
        DROP TABLE IF EXISTS singer_in_concert;
        DROP TABLE IF EXISTS concert;
        DROP TABLE IF EXISTS singer;
        DROP TABLE IF EXISTS stadium;

        CREATE TABLE stadium (
            Stadium_ID  INTEGER PRIMARY KEY,
            Location    TEXT NOT NULL,
            Name        TEXT NOT NULL,
            Capacity    INTEGER,
            Highest     INTEGER,
            Lowest      INTEGER,
            Average     INTEGER
        );

        CREATE TABLE singer (
            Singer_ID          INTEGER PRIMARY KEY,
            Name               TEXT NOT NULL,
            Country            TEXT,
            Song_Name          TEXT,
            Song_release_year  TEXT,
            Age                INTEGER,
            Is_male            TEXT
        );

        CREATE TABLE concert (
            concert_ID    INTEGER PRIMARY KEY,
            concert_Name  TEXT NOT NULL,
            Theme         TEXT,
            Stadium_ID    INTEGER REFERENCES stadium(Stadium_ID),
            Year          TEXT
        );

        CREATE TABLE singer_in_concert (
            concert_ID  INTEGER REFERENCES concert(concert_ID),
            Singer_ID   INTEGER REFERENCES singer(Singer_ID),
            PRIMARY KEY (concert_ID, Singer_ID)
        );

        INSERT INTO stadium VALUES (1,'Raith Rovers','Stark''s Park',10104,4812,1294,2106);
        INSERT INTO stadium VALUES (2,'Ayr United','Somerset Park',11998,2363,1057,1477);
        INSERT INTO stadium VALUES (3,'East Fife','Bayview Stadium',2000,1980,533,864);

        INSERT INTO singer VALUES (1,'Joe Sharp','Netherlands','You','1992',52,'F');
        INSERT INTO singer VALUES (2,'Timbaland','United States','Apologize','2006',32,'M');
        INSERT INTO singer VALUES (3,'Justin Brown','France','Hey Oh','2012',29,'M');
        INSERT INTO singer VALUES (4,'Rose',NULL,'Sun','2003',41,'F');

        INSERT INTO concert VALUES (1,'Auditions','Free choice',1,'2014');
        INSERT INTO concert VALUES (2,'Super bootcamp','Free choice 2',2,'2014');
        INSERT INTO concert VALUES (3,'Home Visits','Bleeding Love',2,'2015');
        INSERT INTO concert VALUES (4,'Week 1','Wide Awake',1,'2014');

        INSERT INTO singer_in_concert VALUES (1,2);
        INSERT INTO singer_in_concert VALUES (1,3);
        INSERT INTO singer_in_concert VALUES (2,3);
        INSERT INTO singer_in_concert VALUES (3,4);
        INSERT INTO singer_in_concert VALUES (4,1);
    """)
    con.commit()
    con.close()
    print(f"Created: {db_path}")


# ── pets_1 ────────────────────────────────────────────────────────────────────

def create_pets_1() -> None:
    db_path = DB_DIR / "pets_1" / "pets_1.db"
    db_path.parent.mkdir(exist_ok=True)

    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.executescript("""
        DROP TABLE IF EXISTS Has_Pet;
        DROP TABLE IF EXISTS Pets;
        DROP TABLE IF EXISTS Student;

        CREATE TABLE Student (
            StuID    INTEGER PRIMARY KEY,
            LName    TEXT NOT NULL,
            Fname    TEXT NOT NULL,
            Age      INTEGER,
            Sex      TEXT,
            Major    INTEGER,
            Advisor  INTEGER,
            city_code TEXT
        );

        CREATE TABLE Pets (
            PetID    INTEGER PRIMARY KEY,
            PetType  TEXT NOT NULL,
            pet_age  INTEGER,
            weight   REAL
        );

        CREATE TABLE Has_Pet (
            StuID  INTEGER REFERENCES Student(StuID),
            PetID  INTEGER REFERENCES Pets(PetID),
            PRIMARY KEY (StuID, PetID)
        );

        INSERT INTO Student VALUES (1001,'Smith','Linda',18,'F',600,1121,'BAL');
        INSERT INTO Student VALUES (1002,'Kim','Tracy',19,'F',600,7712,'HKG');
        INSERT INTO Student VALUES (1003,'Jones','Shiela',21,'F',600,7792,'WAS');
        INSERT INTO Student VALUES (1004,'Kumar','Dinesh',20,'M',600,8423,'CHI');
        INSERT INTO Student VALUES (1005,'Gomez','Carlos',21,'M',50,1121,'CHI');

        INSERT INTO Pets VALUES (1,'cat',3,11.3);
        INSERT INTO Pets VALUES (2,'dog',4,9.3);
        INSERT INTO Pets VALUES (3,'dog',1,3.4);
        INSERT INTO Pets VALUES (4,'cat',5,12.1);

        INSERT INTO Has_Pet VALUES (1001,1);
        INSERT INTO Has_Pet VALUES (1001,2);
        INSERT INTO Has_Pet VALUES (1003,3);
        INSERT INTO Has_Pet VALUES (1004,4);
    """)
    con.commit()
    con.close()
    print(f"Created: {db_path}")


# ── car_1 ─────────────────────────────────────────────────────────────────────

def create_car_1() -> None:
    db_path = DB_DIR / "car_1" / "car_1.db"
    db_path.parent.mkdir(exist_ok=True)

    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.executescript("""
        DROP TABLE IF EXISTS cars_data;
        DROP TABLE IF EXISTS car_names;
        DROP TABLE IF EXISTS model_list;
        DROP TABLE IF EXISTS car_makers;
        DROP TABLE IF EXISTS countries;
        DROP TABLE IF EXISTS continents;

        CREATE TABLE continents (
            ContId     INTEGER PRIMARY KEY,
            Continent  TEXT NOT NULL
        );

        CREATE TABLE countries (
            CountryId    INTEGER PRIMARY KEY,
            CountryName  TEXT NOT NULL,
            Continent    INTEGER REFERENCES continents(ContId)
        );

        CREATE TABLE car_makers (
            Id       INTEGER PRIMARY KEY,
            Maker    TEXT,
            FullName TEXT NOT NULL,
            Country  INTEGER REFERENCES countries(CountryId)
        );

        CREATE TABLE model_list (
            ModelId  INTEGER PRIMARY KEY,
            Maker    INTEGER REFERENCES car_makers(Id),
            Model    TEXT NOT NULL
        );

        CREATE TABLE car_names (
            MakeId  INTEGER PRIMARY KEY,
            Model   TEXT REFERENCES model_list(Model),
            Make    TEXT NOT NULL
        );

        CREATE TABLE cars_data (
            Id           INTEGER PRIMARY KEY REFERENCES car_names(MakeId),
            MPG          REAL,
            Cylinders    INTEGER,
            Edispl       REAL,
            Horsepower   REAL,
            Weight       INTEGER,
            Accelerate   REAL,
            Year         INTEGER
        );

        INSERT INTO continents VALUES (1,'america');
        INSERT INTO continents VALUES (2,'europe');
        INSERT INTO continents VALUES (3,'asia');

        INSERT INTO countries VALUES (1,'usa',1);
        INSERT INTO countries VALUES (2,'germany',2);
        INSERT INTO countries VALUES (3,'japan',3);
        INSERT INTO countries VALUES (4,'france',2);

        INSERT INTO car_makers VALUES (1,'chevrolet','General Motors',1);
        INSERT INTO car_makers VALUES (2,'bmw','BMW',2);
        INSERT INTO car_makers VALUES (3,'toyota','Toyota',3);
        INSERT INTO car_makers VALUES (4,'renault','Renault',4);

        INSERT INTO model_list VALUES (1,1,'chevette');
        INSERT INTO model_list VALUES (2,2,'3-series');
        INSERT INTO model_list VALUES (3,3,'corolla');
        INSERT INTO model_list VALUES (4,4,'clio');

        INSERT INTO car_names VALUES (1,'chevette','Chevrolet Chevette');
        INSERT INTO car_names VALUES (2,'3-series','BMW 3-Series');
        INSERT INTO car_names VALUES (3,'corolla','Toyota Corolla');
        INSERT INTO car_names VALUES (4,'clio','Renault Clio');

        INSERT INTO cars_data VALUES (1,29.0,4,97.0,75.0,2171,16.0,1975);
        INSERT INTO cars_data VALUES (2,25.0,4,121.0,115.0,2671,13.5,1975);
        INSERT INTO cars_data VALUES (3,32.0,4,83.0,61.0,2003,19.0,1974);
        INSERT INTO cars_data VALUES (4,28.0,4,97.0,60.0,2130,14.5,1973);
    """)
    con.commit()
    con.close()
    print(f"Created: {db_path}")


if __name__ == "__main__":
    create_concert_singer()
    create_pets_1()
    create_car_1()
    print("\nAll CoSQL fixture databases created in data/cosql/databases/")
