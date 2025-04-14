/****** Object:  Table [dbo].[AzureResourceCost]    Script Date: 2025-04-13 5:30:18 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[AzureResourceCost](
	[SubscriptionName] [nvarchar](500) NULL,
	[SubscriptionId] [nvarchar](500) NULL,
	[ResourceGroup] [nvarchar](500) NULL,
	[ResourceName] [nvarchar](500) NULL,
	[ResourceID] [nvarchar](500) NULL,
	[ConsumedService] [nvarchar](500) NULL,
	[MeterCategory] [nvarchar](500) NULL,
	[MeterSubcategory] [nvarchar](500) NULL,
	[Location] [nvarchar](500) NULL,
	[BillingMonth] [nvarchar](50) NULL,
	[CostCenter] [nvarchar](50) NULL,
	[Cost] [nvarchar](50) NULL
) ON [PRIMARY]
GO

/****** Object:  Table [dbo].[BillingAccountCost]    Script Date: 2025-04-13 5:30:31 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[BillingAccountCost](
	[SubscriptionName] [nvarchar](500) NULL,
	[SubscriptionId] [nvarchar](500) NULL,
	[TotalCost] [nvarchar](500) NULL,
	[Month] [nvarchar](50) NULL,
	[Year] [nvarchar](50) NULL,
	[Date] [nvarchar](50) NULL
) ON [PRIMARY]
GO

/****** Object:  Table [dbo].[ProcessCheckpoint]    Script Date: 2025-04-13 5:30:58 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[ProcessCheckpoint](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[LastProcessedFile] [nvarchar](500) NULL,
	[LastProcessedRow] [int] NULL,
	[LastProcessedTimestamp] [datetime] NULL,
PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[ProcessCheckpoint] ADD  DEFAULT (getdate()) FOR [LastProcessedTimestamp]
GO

/****** Object:  Table [dbo].[SubscriptionCost]    Script Date: 2025-04-13 5:31:21 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[SubscriptionCost](
	[SubscriptionName] [nvarchar](500) NULL,
	[SubscriptionId] [nvarchar](500) NULL,
	[AzureCost] [nvarchar](500) NULL,
	[ResourceCount] [nvarchar](500) NULL,
	[Month] [nvarchar](50) NULL,
	[Year] [nvarchar](50) NULL,
	[Date] [nvarchar](50) NULL
) ON [PRIMARY]
GO




