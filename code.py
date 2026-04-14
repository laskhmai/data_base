-- Drop and recreate with IDENTITY
DROP TABLE [MongoDB].[Process];

CREATE TABLE [MongoDB].[Process]
(
    ProcessKey          INT             NOT NULL IDENTITY(1,1),
    OrgKey              INT             NOT NULL,
    ProjectKey          INT             NULL,
    ClusterKey          INT             NULL,
    Name                NVARCHAR(500)   NOT NULL,
    ReplicaSetName      NVARCHAR(500)   NULL,
    ProcessId           NVARCHAR(500)   NOT NULL,
    ProcessType         NVARCHAR(500)   NULL,
    Links               NVARCHAR(500)   NULL,
    UserAlias           NVARCHAR(500)   NULL,
    Version             NVARCHAR(500)   NULL,
    ProcessCreatedDate  DATETIME2(3)    NULL,
    ProcessUpdatedDate  DATETIME2(3)    NULL,
    VerifiedUtc         DATETIME2(3)    NULL,
    AuditUtc            DATETIME2(3)    NOT NULL,
    AuditUser           NVARCHAR(100)   NULL,
    IsDeleted           BIT             NOT NULL DEFAULT 0
);