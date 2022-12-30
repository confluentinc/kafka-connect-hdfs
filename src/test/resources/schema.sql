CREATE TABLE IF NOT EXISTS TESTTABLE
(
    `ID` INT AUTO_INCREMENT NOT NULL,
    `MY_DATE` DATE,
    `MY_BLOB` BLOB,
    `MY_CLOB` CLOB,
    `MY_OTHER_CLOB` CLOB NOT NULL,
    `MY_VARCHAR` VARCHAR(10),
    PRIMARY KEY (`ID`)
);
