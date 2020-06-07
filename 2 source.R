
# Start overall timer #
overStart <- Sys.time()
cat(
  paste(
    "Using", currYear, "as base year.", "\n"
  )
)
cat(paste(" \n"))

############
##| Data |##
############

cat(
  paste(
    "Preparing data ... "
  )
)

# Read post drawing data #
aaPostDrawAll <- fread(paste0("1 Data/", postDrawingFile),
                       header = TRUE,
                       sep = ",",
                       data.table = FALSE,
                       stringsAsFactors = FALSE)

# Drawing applicants #
drawApps <- aaPostDrawAll[aaPostDrawAll$EntryMethod == "Drawing", ]
drawApps <- drawApps[c("EventID", "CustomerID", "Status", "RegistrationDate")]

# Pull customer created dates #
query <- paste0(
"SELECT 
	[CustomerID]
	,CAST([CreatedDate] AS DATE) AS CreatedDate 
FROM [dbo].[Customers]"
)
system.time(custCreate <- dbGetQuery(zzCon, query))
custCreate$CustomerID <- as.integer(custCreate$CustomerID)
custCreate$CreatedDate <- as.character(custCreate$CreatedDate)
rm(query)

# Match created dates to drawing applicants #
drawApps$CreatedDate <- custCreate$CreatedDate[match(drawApps$CustomerID, custCreate$CustomerID)]
rm(custCreate)

# Format dates #
drawApps$RegistrationDate <- mdy(drawApps$RegistrationDate)
drawApps$CreatedDate <- ymd(drawApps$CreatedDate)

cat(
  paste(
    "done.", "\n", sep = " "
  )
)
cat(paste(" \n"))

#####################
##| Account Types |##
#####################

cat(
  paste(
    "Processing previous year's activity ... "
  )
)

###| New Accounts |###
#drawApps$AccountType <- NA ## <<<<< For reruns only
drawApps$AccountType[drawApps$CreatedDate >= appOpen & drawApps$CreatedDate <= appClose] <- "New"

###| Dormant and Active Accounts |###
#| Loop not new runners, get previous activity based on parameters, label as dormant if there is none, label active if there is |#
# Initiate progress bar #
pb <- winProgressBar(title = "Progress", 
                     label = "0% Done", 
                     min = 0, 
                     max = 100, 
                     initial = 0)
# Start timer #
loopStart <- Sys.time()
# Loop race registrants, pull reg data from Fred, assign runner type #
for (i in 1:nrow(drawApps)){
  # Test if account is not new #
  if (is.na(drawApps$AccountType[i])){
    # Build query #
    query <- paste0(
      "SELECT 
        [CustomerID]
        ,[EventID]
        ,[CreatedDate] AS RegistrationDate 
      FROM [dbo].[CustomerEventReg] 
        WHERE [CustomerID] = ", as.character(drawApps$CustomerID[i]), " AND CAST([CreatedDate] AS DATE) >= CAST('",
      ifelse(
        !is.na(drawApps$RegistrationDate[i] - (years(y) + months(m))),               
        as.character(drawApps$RegistrationDate[i] - (years(y) + months(m))),
        ifelse(
          is.na(drawApps$RegistrationDate[i] - days(1) - (years(y) + months(m))),
          as.character(drawApps$RegistrationDate[i] - days(3) - (years(y) + months(m))),  # Fucking February
          as.character(drawApps$RegistrationDate[i] - days(1) - (years(y) + months(m)))
        )
      ), "' AS DATE) AND CAST([CreatedDate] AS DATE) < CAST('", as.character(drawApps$RegistrationDate[i]), "' AS DATE)"
    )
    temp <- dbGetQuery(zzCon, query)
    temp$CustomerID <- as.integer(temp$CustomerID)
    temp$EventID <- as.integer(temp$EventID)
    # Test if there is activity in previous year #
    if (nrow(temp) == 0){
      # Mark dormant if none #
      drawApps$AccountType[i] <- "Dormant"
    } else {
      # Test if there is only 1 if it is previous marathon #
      if (nrow(temp) == 1 & temp$EventID == prevID){
        # Mark marathon app only if true #
        drawApps$AccountType[i] <- paste(prevYear, "Only")
      } else {
        # Otherwise mark as active #
        drawApps$AccountType[i] <- "Active"
      }
    }
  }
  # Update progress bar
  info <- sprintf("%s%% Done", round((i/nrow(drawApps)) * 100,
                                     digits = 1))
  setWinProgressBar(pb, (i/nrow(drawApps)) * 100, label = info)
}
# Stop timer, add to tracker, display processing time and overall progress #
loopEnd <- Sys.time()
loopTime <- round(as.numeric(loopEnd) - as.numeric(loopStart), 0)
cat(
  paste(
    "runner activity processed and done in:", seconds_to_period(loopTime), "\n", sep = " "
  )
)
cat(paste(" \n"))

# Close progress bar #
close(pb)
# Clean up #
rm(i, loopStart, loopEnd, loopTime, info, pb, temp, query)

###############################
##| Following Year Activity |##
###############################

cat(
  paste(
    "Processing following year's activity ... "
  )
)

#| Loop all runners, get following year activity, label as application only if there is none, label as registered for other events if there is |#

## FOR RE-RUNS ONLY - format date
#drawApps$RegistrationDate <- ymd(drawApps$RegistrationDate)
# Reset activity indicator #
#drawApps$PostRegActivity <- NA

# Start results dataset #
drawActivity <- data.frame(
  CustomerID = integer(),
  EventID = integer(),
  CustomerEventStatus = integer(),
  EventName = character(),
  EventStartDate = character(),
  Distance = numeric(),
  DistanceMeasurementType = integer()
)

# Initiate progress bar #
pb <- winProgressBar(title = "Progress", 
                     label = "0% Done", 
                     min = 0, 
                     max = 100, 
                     initial = 0)
# Start timer #
loopStart <- Sys.time()
# Loop race registrants, pull reg data from Fred, assign runner type #
for (i in 1:nrow(drawApps)){
  # Build query #
  query <- paste0(
  "SELECT
	  regs.CustomerID
	  ,regs.EventID
	  ,regs.CustomerEventStatus
    ,CAST(regs.CreatedDate AS DATE) AS RegDateOnly
	  ,races.EventName
	  ,CAST(races.EventStartDate AS DATE) AS EventStartDate
	  ,races.Distance
	  ,races.DistanceMeasurementType
	FROM [dbo].[CustomerEventReg] regs

  LEFT JOIN [dbo].[Events] races
	  ON regs.EventID = races.EventID

  WHERE regs.CustomerID = ", as.character(drawApps$CustomerID[i]),
	  "AND (CAST(regs.CreatedDate AS DATE) > CAST('", as.character(drawApps$RegistrationDate[i]),"' AS DATE)	
		  AND CAST(regs.CreatedDate AS DATE) <= CAST('", as.character(drawApps$RegistrationDate[i] + years(1)),"'AS DATE))"
  )
  temp <- dbGetQuery(zzCon, query)
  temp$CustomerID <- as.integer(temp$CustomerID)
  temp$EventID <- as.integer(temp$EventID)
  # Test if there is activity in following year #
  if (nrow(temp) == 0){
    # Mark app only if none #
    drawApps$PostRegActivity[i] <- "Application Only"
  } else {
    # Test if there is only 1 and if it is next marathon #
    if (nrow(temp) == 1 & temp$EventID == nextID){
      # Mark marathon apps only if true #
      drawApps$PostRegActivity[i] <- paste("Application &", nextYear,"Only")
      # Add activity to results set #
      drawActivity <- rbind(drawActivity, temp)
    } else {
      # Otherwise mark as regged for other events #
      drawApps$PostRegActivity[i] <- "Registered for Events"
      # Add activity to results set #
      drawActivity <- rbind(drawActivity, temp)
    } 
  }
  # Update progress bar
  info <- sprintf("%s%% Done", round((i/nrow(drawApps)) * 100,
                                     digits = 1))
  setWinProgressBar(pb, (i/nrow(drawApps)) * 100, label = info)
}
# Stop timer, add to tracker, display processing time and overall progress #
loopEnd <- Sys.time()
loopTime <- round(as.numeric(loopEnd) - as.numeric(loopStart), 0)
cat(
  paste(
    "runner activity processed and done in:", seconds_to_period(loopTime), "\n", sep = " "
  )
)
cat(paste(" \n"))
# Close progress bar #
close(pb)
# Clean up #
rm(i, loopStart, loopEnd, loopTime, info, pb, y, m, temp, query)

####################
##| Demographics |##
####################

cat(
  paste(
    "Adding demographics ... "
  )
)

# Domestic/international #
drawApps$DomInt <- ifelse(
  aaPostDrawAll$CountryRes[match(drawApps$CustomerID, aaPostDrawAll$CustomerID)] == "United States", "Domestic", "International"
)

# Surrounding states #
ss <- c("NY", "NJ", "PA", "CT", "MA")
drawApps$Surround <- ifelse(
  aaPostDrawAll$UsState[match(drawApps$CustomerID, aaPostDrawAll$CustomerID)] %in% ss, "Surrounding States", "Outside Surrounding States"
)
rm(ss)

cat(
  paste(
    "done", "\n"
  )
)
cat(paste("\n"))

#############################################
##| Final Status / Entry Method / Charity |##
#############################################

cat(
  paste(
    "Adding final status, entry method, and affiliate name ... "
  )
)

# Pull current status, entry method, and charity from FRED #
query <- paste0(
"SELECT
	regs.[CustomerID]
	,regs.[EventID]
	,regs.[CustomerEventStatus]
	,entMeth.Name AS EntryMethod
	,ISNULL(affType.Name, '') AS AffType
	,ISNULL(affs.Name, '') AS Affiliation
FROM [dbo].[CustomerEventReg] regs

LEFT JOIN [dbo].[EntryMethods] entMeth
	ON regs.EntryMethodID = entMeth.EntryMethodID
LEFT JOIN [dbo].[Affiliates] affs
	ON regs.AffiliationID = affs.AffiliateID
LEFT JOIN [dbo].[AffiliationTypes] affType
	ON affs.AffiliationTypeID = affType.AffiliationTypeID

WHERE [EventID] = ", currID
)
system.time(finInfo <- dbGetQuery(zzCon, query))
rm(query)

# Format #
finInfo$CustomerID <- as.integer(finInfo$CustomerID)
finInfo$EventID <- as.integer(finInfo$EventID)

# Label statuses #
finInfo$Status[finInfo$CustomerEventStatus == 0] <- "Not Accepted"
finInfo$Status[finInfo$CustomerEventStatus == 1] <- "Accepted"
finInfo$Status[finInfo$CustomerEventStatus == 2] <- "Pending"
finInfo$Status[finInfo$CustomerEventStatus == 3] <- "Removed"
finInfo$Status[finInfo$CustomerEventStatus == 4] <- "Cancelled"
finInfo$Status[finInfo$CustomerEventStatus == 5] <- "Withdrawn"
finInfo$Status[finInfo$CustomerEventStatus == 6] <- "Disqualified"
finInfo$Status[finInfo$CustomerEventStatus == 7] <- "Did Not Start"
finInfo$Status[finInfo$CustomerEventStatus == 8] <- "Did Not Finish"
finInfo$Status[finInfo$CustomerEventStatus == 9] <- "Completed"
finInfo$Status[finInfo$CustomerEventStatus == 10] <- "Transfer"
finInfo$Status[finInfo$CustomerEventStatus == 11] <- "Refund"
finInfo$Status[finInfo$CustomerEventStatus == 12] <- "Withdrawn from Drawing"

# Match current status to original drawing applicants set #
drawApps$FinStatus <- finInfo$Status[match(drawApps$CustomerID, finInfo$CustomerID)]

# Match entry method if completed #
#drawApps$FinEntMethod <- ifelse(
#  drawApps$FinStatus == "Completed",
#  finInfo$EntryMethod[match(drawApps$CustomerID, finInfo$CustomerID)], ""
#)

# Match entry method 
drawApps$FinEntMethod <- finInfo$EntryMethod[match(drawApps$CustomerID, finInfo$CustomerID)]

# Match affiliate name #
drawApps$Affiliate <- finInfo$Affiliation[match(drawApps$CustomerID, finInfo$CustomerID)]
rm(finInfo)

cat(
  paste(
    "done.", "\n"
  )
)
cat(paste(" \n"))

############################################
##| Following Year Status / Entry Method |##
############################################

cat(
  paste(
    "Adding following marathon registration status ... "
  )
)

# Pull following year status and entry method from FRED #
query <- paste0(
"SELECT
	regs.[CustomerID]
  ,regs.[EventID]
  ,regs.[CustomerEventStatus]
  ,entMeth.Name AS EntryMethod
FROM [dbo].[CustomerEventReg] regs
  
LEFT JOIN [dbo].[EntryMethods] entMeth
  ON regs.EntryMethodID = entMeth.EntryMethodID
  
WHERE [EventID] = ", nextID
)
system.time(followYear <- dbGetQuery(zzCon, query))

# Format #
followYear$CustomerID <- as.integer(followYear$CustomerID)
followYear$EventID <- as.integer(followYear$EventID)

# Label status #
followYear$Status[followYear$CustomerEventStatus == 0] <- "Not Accepted"
followYear$Status[followYear$CustomerEventStatus == 1] <- "Accepted"
followYear$Status[followYear$CustomerEventStatus == 2] <- "Pending"
followYear$Status[followYear$CustomerEventStatus == 3] <- "Removed"
followYear$Status[followYear$CustomerEventStatus == 4] <- "Cancelled"
followYear$Status[followYear$CustomerEventStatus == 5] <- "Withdrawn"
followYear$Status[followYear$CustomerEventStatus == 6] <- "Disqualified"
followYear$Status[followYear$CustomerEventStatus == 7] <- "Did Not Start"
followYear$Status[followYear$CustomerEventStatus == 8] <- "Did Not Finish"
followYear$Status[followYear$CustomerEventStatus == 9] <- "Completed"
followYear$Status[followYear$CustomerEventStatus == 10] <- "Transfer"
followYear$Status[followYear$CustomerEventStatus == 11] <- "Refund"
followYear$Status[followYear$CustomerEventStatus == 12] <- "Withdrawn from Drawing"

# Match current status if possible #
drawApps$CurrMarStat <- ifelse(
  !is.na(followYear$Status[match(drawApps$CustomerID, followYear$CustomerID)]),
  followYear$Status[match(drawApps$CustomerID, followYear$CustomerID)], "Not Registered"
) 

# Match current entry method if registered #
drawApps$CurrMarEntMeth <- ifelse(
  drawApps$CurrMarStat != "Not Registered",
  followYear$EntryMethod[match(drawApps$CustomerID, followYear$CustomerID)], ""
)
rm(followYear)

cat(
  paste(
    "done.", "\n"
  )
)
cat(paste(" \n"))

########################
##| Activity revenue |##
########################

cat(
  paste(
    "Adding prices to registration activity ... "
  )
)

# Label statuses #
drawActivity$CustomerEventStatus[drawActivity$CustomerEventStatus == 0] <- "Not Accepted"
drawActivity$CustomerEventStatus[drawActivity$CustomerEventStatus == 1] <- "Accepted"
drawActivity$CustomerEventStatus[drawActivity$CustomerEventStatus == 2] <- "Pending"
drawActivity$CustomerEventStatus[drawActivity$CustomerEventStatus == 3] <- "Removed"
drawActivity$CustomerEventStatus[drawActivity$CustomerEventStatus == 4] <- "Cancelled"
drawActivity$CustomerEventStatus[drawActivity$CustomerEventStatus == 5] <- "Withdrawn"
drawActivity$CustomerEventStatus[drawActivity$CustomerEventStatus == 6] <- "Disqualified"
drawActivity$CustomerEventStatus[drawActivity$CustomerEventStatus == 7] <- "Did Not Start"
drawActivity$CustomerEventStatus[drawActivity$CustomerEventStatus == 8] <- "Did Not Finish"
drawActivity$CustomerEventStatus[drawActivity$CustomerEventStatus == 9] <- "Completed"
drawActivity$CustomerEventStatus[drawActivity$CustomerEventStatus == 10] <- "Transfer"
drawActivity$CustomerEventStatus[drawActivity$CustomerEventStatus == 11] <- "Refund"
drawActivity$CustomerEventStatus[drawActivity$CustomerEventStatus == 12] <- "Withdrawn from Drawing"

# Remove duplicates #
drawActivity$uid <- paste(drawActivity$CustomerID, drawActivity$EventID, sep = "-")
drawActivity <- drawActivity[!duplicated(drawActivity$uid), ]
drawActivity$uid <- NULL

# Pull transactions #
query <- paste0(
"SELECT DISTINCT
	transDeets.CustomerTransactionID
	,trans.CustomerID
	,transDeets.EventID
	,CASE
		WHEN transDeets.ItemType = 1 THEN 'Membership'
		WHEN transDeets.ItemType = 2 THEN 'Event'
		WHEN transDeets.ItemType = 3 THEN 'Merchandise'
		WHEN transDeets.ItemType = 4 THEN 'Classes'
		WHEN transDeets.ItemType = 5 THEN 'Donation'
		WHEN transDeets.ItemType = 6 THEN 'Shipping Charges'
		WHEN transDeets.ItemType = 7 THEN 'Sales Tax'
		WHEN transDeets.ItemType = 8 THEN 'Class Discount'
		WHEN transDeets.ItemType = 9 THEN 'Extra Payment'
		WHEN transDeets.ItemType = 10 THEN 'CC Surcharge'
		WHEN transDeets.ItemType = 11 THEN 'Gift of Membership'
		WHEN transDeets.ItemType = 12 THEN 'Voucher Code'
		WHEN transDeets.ItemType = 14 THEN 'Insurance'
		ELSE 'Unknown'
	 END AS 'ItemType'
	,transDeets.Price
	,trans.TransactionDate
FROM [dbo].[CustomerTransactionDetails] transDeets

LEFT JOIN [dbo].[CustomerTransactions] trans
	ON transDeets.CustomerTransactionID = trans.CustomerTransactionID

WHERE CAST(trans.TransactionDate AS DATE) >= CAST('", appOpen, "' AS DATE) AND transDeets.ItemType = 2"
)
system.time(eventTrans <- dbGetQuery(zzCon, query))
eventTrans$CustomerTransactionID <- as.integer(eventTrans$CustomerTransactionID)
eventTrans$CustomerID <- as.integer(eventTrans$CustomerID)
eventTrans$EventID <- as.integer(eventTrans$EventID)

# Create UID for activity and transactions #
drawActivity$uid <- paste(drawActivity$CustomerID, drawActivity$EventID, sep = "-")
eventTrans$uid <- paste(eventTrans$CustomerID, eventTrans$EventID, sep = "-")

# Match price to activity #
drawActivity$Price <- eventTrans$Price[match(drawActivity$uid, eventTrans$uid)]

cat(
  paste(
    "done.", "\n"
  )
)
cat(paste(" \n"))

##############
##| Export |##
##############

cat(
  paste(
    "Exporting data ... "
  )
)

write.csv(drawApps,
          paste0("1 Data/progressSetDrawApps", currYear,".csv"),
          quote = TRUE,
          row.names = FALSE,
          na = "")
write.csv(drawActivity,
          paste0("1 Data/progressSetDrawActivity", currYear, ".csv"),
          quote = TRUE,
          row.names = FALSE)

cat(
  paste(
    "done.", "\n"
  )
)
cat(paste(" \n"))

# Stop timer, display overall processing time #
overEnd <- Sys.time()
overTime <- round(as.numeric(overEnd) - as.numeric(overStart), 0)
cat(
  paste(
    "Overall processing time:", seconds_to_period(overTime), "\n", sep = " "
  )
)
cat(paste(" \n"))

