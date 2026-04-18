CREATE DATABASE IF NOT EXISTS purple_bank;
USE purple_bank;

CREATE TABLE Taxon
(
    taxon_id INT PRIMARY KEY AUTO_INCREMENT,
    kingdom VARCHAR(100),
    phylum VARCHAR(100),
    class VARCHAR(100),
    `order` VARCHAR(100),
    family VARCHAR(100),
    genus VARCHAR(100)
);


CREATE TABLE Organism
(
    organism_id INT PRIMARY KEY AUTO_INCREMENT,
    scientific_name VARCHAR(255) NOT NULL,
    taxon_id INT,
    FOREIGN KEY (taxon_id) REFERENCES Taxon(taxon_id)
);

CREATE TABLE Sequence
(
    accession VARCHAR(50) PRIMARY KEY,
    version VARCHAR(50),
    locus VARCHAR(50),
    definition TEXT,
    organism_id INT,
    length INT,
    mol_type VARCHAR(50),
    sequence LONGTEXT,
    FOREIGN KEY (organism_id) REFERENCES Organism(organism_id)
);


CREATE TABLE DNA (
    accession VARCHAR(50) PRIMARY KEY,
    dna_type VARCHAR(50),
    FOREIGN KEY (accession) REFERENCES Sequence(accession)
);


CREATE TABLE RNA (
    accession VARCHAR(50) PRIMARY KEY,
    rna_type VARCHAR(50),
    FOREIGN KEY (accession) REFERENCES Sequence(accession)
);

CREATE TABLE Feature
(
    feature_id INT PRIMARY KEY AUTO_INCREMENT,
    accession VARCHAR(50),
    `key` VARCHAR(50),
    location VARCHAR(100),
    gene VARCHAR(50),
    product TEXT,
    translation TEXT,
    note TEXT,
    FOREIGN KEY (accession) REFERENCES Sequence(accession)
);


CREATE TABLE Reference
(
    ref_id INT PRIMARY KEY AUTO_INCREMENT,
    accession VARCHAR(50),
    title TEXT,
    journal VARCHAR(255),
    year INT,
    pmid VARCHAR(50),
    FOREIGN KEY (accession) REFERENCES Sequence(accession)
);

CREATE TABLE Author
(
    author_id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100),
    affiliation VARCHAR(255)
);

CREATE TABLE Ref_Sequence
(
    ref_id INT,
    accession VARCHAR(50),
    PRIMARY KEY  (ref_id, accession),
    FOREIGN KEY  (ref_id) REFERENCES Reference(ref_id),
    FOREIGN KEY  (accession) REFERENCES Sequence(accession)
);

CREATE TABLE Ref_Author
(
    ref_id INT,
    author_id INT,
    PRIMARY KEY  (ref_id, author_id),
    FOREIGN KEY  (ref_id) REFERENCES Reference(ref_id),
    FOREIGN KEY  (author_id) REFERENCES Author(author_id)
);
