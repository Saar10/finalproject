# Load packages
library(dplyr)
library(readr)
library(xlsx)
library(zoo)

# Set working directory
setwd("/Users/Saar/Documents/Final Project/Data")

# Load stocks data
# IBEX35 Spanish stock market index
ibex_prices <- read_csv("IBEX.csv")
# BBVA (finance sector)
bbva_prices <- read_csv("BBVA_Prices.csv")
# Endesa (utilities sector)
ele_prices <- read_csv("ELE_Prices.csv")
# Ezentis (industrial sector)
eze_prices <- read_csv("EZE_Prices.csv")
# Gas Natural (utilities sector)
gas_prices <- read_csv("GAS_Prices.csv")
# Repsol (energy sector)
rep_prices <- read_csv("REP_Prices.csv")
# Santander (finance sector)
san_prices <- read_csv("SAN_Prices.csv")
# Telefonica (communications sector)
tef_prices <- read_csv("TEF_Prices.csv")
# Zeltia (healtcare sector)
zel_prices <- read_csv("ZEL_Prices.csv")

# Load investors data
# BBVA (finance sector)
bbva_investors <- read_csv("BBVA_Investors.csv")
# Endesa (utilities sector)
ele_investors <- read_csv("ELE_Investors.csv")
# Ezentis (industrial sector)
eze_investors <- read_csv("EZE_Investors.csv")
# Gas Natural (utilities sector)
gas_investors <- read_csv("GAS_Investors.csv")
# Repsol (energy sector)
rep_investors <- read_csv("REP_Investors.csv")
# Santander (finance sector)
san_investors <- read_csv("SAN_Investors.csv")
# Telefonica (communications sector)
tef_investors <- read_csv("TEF_Investors.csv")
# Zeltia (healtcare sector)
zel_investors <- read_csv("ZEL_Investors.csv")

# Fix IBEX column types
ibex_cols <- c(2, 3, 4, 5, 6, 7)
ibex_prices[ , ibex_cols] <- apply(ibex_prices[ , ibex_cols], 2, function(x) as.numeric(as.character(x)))
# IBEX column name change
ibex_prices <- ibex_prices %>%
  rename(date = Date)
# Replace each NA in IBEX price with most recent non-NA
ibex_prices$Open <- na.locf(ibex_prices$Open)
ibex_prices$Close <- na.locf(ibex_prices$Close)

# Add daily price change
ibex_prices <- ibex_prices %>%
  mutate(daily_change = Open - lag(Open), ibex_percent_change = (daily_change / lag(Open)))
bbva_prices <- bbva_prices %>%
  mutate(daily_change = open - lag(open), percent_change = (daily_change / lag(open)))
ele_prices <- ele_prices %>%
  mutate(daily_change = open - lag(open), percent_change = (daily_change / lag(open)))
eze_prices <- eze_prices %>%
  mutate(daily_change = open - lag(open), percent_change = (daily_change / lag(open)))
gas_prices <- gas_prices %>%
  mutate(daily_change = open - lag(open), percent_change = (daily_change / lag(open)))
rep_prices <- rep_prices %>%
  mutate(daily_change = open - lag(open), percent_change = (daily_change / lag(open)))
san_prices <- san_prices %>%
  mutate(daily_change = open - lag(open), percent_change = (daily_change / lag(open)))
tef_prices <- tef_prices %>%
  mutate(daily_change = open - lag(open), percent_change = (daily_change / lag(open)))
zel_prices <- zel_prices %>%
  mutate(daily_change = open - lag(open), percent_change = (daily_change / lag(open)))

# Add daily position change
bbva_investors <- bbva_investors %>%
  mutate(daily_position_change = case_when(
    investor_id == lag(investor_id) ~ position - lag(position),
    investor_id != lag(investor_id) ~ 0))
ele_investors <- ele_investors %>% 
  mutate(daily_position_change = case_when(
    investor_id == lag(investor_id) ~ position - lag(position),
    investor_id != lag(investor_id) ~ 0))
eze_investors <- eze_investors %>% 
  mutate(daily_position_change = case_when(
    investor_id == lag(investor_id) ~ position - lag(position),
    investor_id != lag(investor_id) ~ 0))
gas_investors <- gas_investors %>% 
  mutate(daily_position_change = case_when(
    investor_id == lag(investor_id) ~ position - lag(position),
    investor_id != lag(investor_id) ~ 0))
rep_investors <- rep_investors %>% 
  mutate(daily_position_change = case_when(
    investor_id == lag(investor_id) ~ position - lag(position),
    investor_id != lag(investor_id) ~ 0))
san_investors <- san_investors %>% 
  mutate(daily_position_change = case_when(
    investor_id == lag(investor_id) ~ position - lag(position),
    investor_id != lag(investor_id) ~ 0))
tef_investors <- tef_investors %>% 
  mutate(daily_position_change = case_when(
    investor_id == lag(investor_id) ~ position - lag(position),
    investor_id != lag(investor_id) ~ 0))
zel_investors <- zel_investors %>% 
  mutate(daily_position_change = case_when(
    investor_id == lag(investor_id) ~ position - lag(position),
    investor_id != lag(investor_id) ~ 0))

# Separate date and daily change
ibex_daily_change <- ibex_prices %>%
  select(date, ibex_percent_change)
bbva_daily_change <- bbva_prices %>%
  select(date, percent_change)
ele_daily_change <- ele_prices %>%
  select(date, percent_change)
eze_daily_change <- eze_prices %>%
  select(date, percent_change)
gas_daily_change <- gas_prices %>%
  select(date, percent_change)
rep_daily_change <- rep_prices %>%
  select(date, percent_change)
san_daily_change <- san_prices %>%
  select(date, percent_change)
tef_daily_change <- tef_prices %>%
  select(date, percent_change)
zel_daily_change <- zel_prices %>%
  select(date, percent_change)

# Join daily change in stock price
bbva_investors <- bbva_investors %>%
  inner_join(bbva_daily_change, by = "date")
ele_investors <- ele_investors %>%
  inner_join(ele_daily_change, by = "date")
eze_investors <- eze_investors %>%
  inner_join(eze_daily_change, by = "date")
gas_investors <- gas_investors %>%
  inner_join(gas_daily_change, by = "date")
rep_investors <- rep_investors %>%
  inner_join(rep_daily_change, by = "date")
san_investors <- san_investors %>%
  inner_join(san_daily_change, by = "date")
tef_investors <- tef_investors %>%
  inner_join(tef_daily_change, by = "date")
zel_investors <- zel_investors %>%
  inner_join(ele_daily_change, by = "date")

# Join daily change in IBEX
bbva_investors <- bbva_investors %>%
  inner_join(ibex_daily_change, by = "date")
ele_investors <- ele_investors %>%
  inner_join(ibex_daily_change, by = "date")
eze_investors <- eze_investors %>%
  inner_join(ibex_daily_change, by = "date")
gas_investors <- gas_investors %>%
  inner_join(ibex_daily_change, by = "date")
rep_investors <- rep_investors %>%
  inner_join(ibex_daily_change, by = "date")
san_investors <- san_investors %>%
  inner_join(ibex_daily_change, by = "date")
tef_investors <- tef_investors %>%
  inner_join(ibex_daily_change, by = "date")
zel_investors <- zel_investors %>%
  inner_join(ibex_daily_change, by = "date")

# Add binary bought/sold columns
bbva_investors <- bbva_investors %>%
  mutate(bought = if_else(daily_position_change > 0, 1, 0),
         sold = if_else(daily_position_change < 0, 1, 0))
ele_investors <- ele_investors %>%
  mutate(bought = if_else(daily_position_change > 0, 1, 0),
         sold = if_else(daily_position_change < 0, 1, 0))
eze_investors <- eze_investors %>%
  mutate(bought = if_else(daily_position_change > 0, 1, 0),
         sold = if_else(daily_position_change < 0, 1, 0))
gas_investors <- gas_investors %>%
  mutate(bought = if_else(daily_position_change > 0, 1, 0),
         sold = if_else(daily_position_change < 0, 1, 0))
rep_investors <- rep_investors %>%
  mutate(bought = if_else(daily_position_change > 0, 1, 0),
         sold = if_else(daily_position_change < 0, 1, 0))
san_investors <- san_investors %>%
  mutate(bought = if_else(daily_position_change > 0, 1, 0),
         sold = if_else(daily_position_change < 0, 1, 0))
tef_investors <- tef_investors %>%
  mutate(bought = if_else(daily_position_change > 0, 1, 0),
         sold = if_else(daily_position_change < 0, 1, 0))
zel_investors <- zel_investors %>%
  mutate(bought = if_else(daily_position_change > 0, 1, 0),
         sold = if_else(daily_position_change < 0, 1, 0))

# Filter investor "18416"
bbva_18416 <- bbva_investors %>%
  filter(investor_id == 18416)
ele_18416 <- ele_investors %>%
  filter(investor_id == 18416)
eze_18416 <- eze_investors %>%
  filter(investor_id == 18416)
gas_18416 <- gas_investors %>%
  filter(investor_id == 18416)
rep_18416 <- rep_investors %>%
  filter(investor_id == 18416)
san_18416 <- san_investors %>%
  filter(investor_id == 18416)
tef_18416 <- tef_investors %>%
  filter(investor_id == 18416)
zel_18416 <- zel_investors %>%
  filter(investor_id == 18416)

# Filter investor "18427"
bbva_18427 <- bbva_investors %>%
  filter(investor_id == 18427)
ele_18427 <- ele_investors %>%
  filter(investor_id == 18427)
eze_18427 <- eze_investors %>%
  filter(investor_id == 18427)
gas_18427 <- gas_investors %>%
  filter(investor_id == 18427)
rep_18427 <- rep_investors %>%
  filter(investor_id == 18427)
san_18427 <- san_investors %>%
  filter(investor_id == 18427)
tef_18427 <- tef_investors %>%
  filter(investor_id == 18427)
zel_18427 <- zel_investors %>%
  filter(investor_id == 18427)

# Export day t+1
write.xlsx(bbva_18416, file = "investor_18416.xlsx", sheetName = "bbva", append = TRUE)
write.xlsx(ele_18416, file = "investor_18416.xlsx", sheetName = "ele", append = TRUE)
write.xlsx(eze_18416, file = "investor_18416.xlsx", sheetName = "eze", append = TRUE)
write.xlsx(gas_18416, file = "investor_18416.xlsx", sheetName = "gas", append = TRUE)
write.xlsx(rep_18416, file = "investor_18416.xlsx", sheetName = "rep", append = TRUE)
write.xlsx(san_18416, file = "investor_18416.xlsx", sheetName = "san", append = TRUE)
write.xlsx(tef_18416, file = "investor_18416.xlsx", sheetName = "tef", append = TRUE)
write.xlsx(zel_18416, file = "investor_18416.xlsx", sheetName = "zel", append = TRUE)
write.xlsx(bbva_18427, file = "investor_18427.xlsx", sheetName = "bbva", append = TRUE)
write.xlsx(ele_18427, file = "investor_18427.xlsx", sheetName = "ele", append = TRUE)
write.xlsx(eze_18427, file = "investor_18427.xlsx", sheetName = "eze", append = TRUE)
write.xlsx(gas_18427, file = "investor_18427.xlsx", sheetName = "gas", append = TRUE)
write.xlsx(rep_18427, file = "investor_18427.xlsx", sheetName = "rep", append = TRUE)
write.xlsx(san_18427, file = "investor_18427.xlsx", sheetName = "san", append = TRUE)
write.xlsx(tef_18427, file = "investor_18427.xlsx", sheetName = "tef", append = TRUE)
write.xlsx(zel_18427, file = "investor_18427.xlsx", sheetName = "zel", append = TRUE)

# Export day t
write.xlsx(bbva_18416, file = "investor_18416_dayt.xlsx", sheetName = "bbva", append = TRUE)
write.xlsx(ele_18416, file = "investor_18416_dayt.xlsx", sheetName = "ele", append = TRUE)
write.xlsx(eze_18416, file = "investor_18416_dayt.xlsx", sheetName = "eze", append = TRUE)
write.xlsx(gas_18416, file = "investor_18416_dayt.xlsx", sheetName = "gas", append = TRUE)
write.xlsx(rep_18416, file = "investor_18416_dayt.xlsx", sheetName = "rep", append = TRUE)
write.xlsx(san_18416, file = "investor_18416_dayt.xlsx", sheetName = "san", append = TRUE)
write.xlsx(tef_18416, file = "investor_18416_dayt.xlsx", sheetName = "tef", append = TRUE)
write.xlsx(zel_18416, file = "investor_18416_dayt.xlsx", sheetName = "zel", append = TRUE)
write.xlsx(bbva_18427, file = "investor_18427_dayt.xlsx", sheetName = "bbva", append = TRUE)
write.xlsx(ele_18427, file = "investor_18427_dayt.xlsx", sheetName = "ele", append = TRUE)
write.xlsx(eze_18427, file = "investor_18427_dayt.xlsx", sheetName = "eze", append = TRUE)
write.xlsx(gas_18427, file = "investor_18427_dayt.xlsx", sheetName = "gas", append = TRUE)
write.xlsx(rep_18427, file = "investor_18427_dayt.xlsx", sheetName = "rep", append = TRUE)
write.xlsx(san_18427, file = "investor_18427_dayt.xlsx", sheetName = "san", append = TRUE)
write.xlsx(tef_18427, file = "investor_18427_dayt.xlsx", sheetName = "tef", append = TRUE)
write.xlsx(zel_18427, file = "investor_18427_dayt.xlsx", sheetName = "zel", append = TRUE)

# Export day t+1 with cutoff
write.xlsx(bbva_18416, file = "investor_18416_cutoff.xlsx", sheetName = "bbva", append = TRUE)
write.xlsx(ele_18416, file = "investor_18416_cutoff.xlsx", sheetName = "ele", append = TRUE)
write.xlsx(eze_18416, file = "investor_18416_cutoff.xlsx", sheetName = "eze", append = TRUE)
write.xlsx(gas_18416, file = "investor_18416_cutoff.xlsx", sheetName = "gas", append = TRUE)
write.xlsx(rep_18416, file = "investor_18416_cutoff.xlsx", sheetName = "rep", append = TRUE)
write.xlsx(san_18416, file = "investor_18416_cutoff.xlsx", sheetName = "san", append = TRUE)
write.xlsx(tef_18416, file = "investor_18416_cutoff.xlsx", sheetName = "tef", append = TRUE)
write.xlsx(zel_18416, file = "investor_18416_cutoff.xlsx", sheetName = "zel", append = TRUE)
write.xlsx(bbva_18427, file = "investor_18427_cutoff.xlsx", sheetName = "bbva", append = TRUE)
write.xlsx(ele_18427, file = "investor_18427_cutoff.xlsx", sheetName = "ele", append = TRUE)
write.xlsx(eze_18427, file = "investor_18427_cutoff.xlsx", sheetName = "eze", append = TRUE)
write.xlsx(gas_18427, file = "investor_18427_cutoff.xlsx", sheetName = "gas", append = TRUE)
write.xlsx(rep_18427, file = "investor_18427_cutoff.xlsx", sheetName = "rep", append = TRUE)
write.xlsx(san_18427, file = "investor_18427_cutoff.xlsx", sheetName = "san", append = TRUE)
write.xlsx(tef_18427, file = "investor_18427_cutoff.xlsx", sheetName = "tef", append = TRUE)
write.xlsx(zel_18427, file = "investor_18427_cutoff.xlsx", sheetName = "zel", append = TRUE)

# Export day t with cutoff
write.xlsx(bbva_18416, file = "investor_18416_dayt_cutoff.xlsx", sheetName = "bbva", append = TRUE)
write.xlsx(ele_18416, file = "investor_18416_dayt_cutoff.xlsx", sheetName = "ele", append = TRUE)
write.xlsx(eze_18416, file = "investor_18416_dayt_cutoff.xlsx", sheetName = "eze", append = TRUE)
write.xlsx(gas_18416, file = "investor_18416_dayt_cutoff.xlsx", sheetName = "gas", append = TRUE)
write.xlsx(rep_18416, file = "investor_18416_dayt_cutoff.xlsx", sheetName = "rep", append = TRUE)
write.xlsx(san_18416, file = "investor_18416_dayt_cutoff.xlsx", sheetName = "san", append = TRUE)
write.xlsx(tef_18416, file = "investor_18416_dayt_cutoff.xlsx", sheetName = "tef", append = TRUE)
write.xlsx(zel_18416, file = "investor_18416_dayt_cutoff.xlsx", sheetName = "zel", append = TRUE)
write.xlsx(bbva_18427, file = "investor_18427_dayt_cutoff.xlsx", sheetName = "bbva", append = TRUE)
write.xlsx(ele_18427, file = "investor_18427_dayt_cutoff.xlsx", sheetName = "ele", append = TRUE)
write.xlsx(eze_18427, file = "investor_18427_dayt_cutoff.xlsx", sheetName = "eze", append = TRUE)
write.xlsx(gas_18427, file = "investor_18427_dayt_cutoff.xlsx", sheetName = "gas", append = TRUE)
write.xlsx(rep_18427, file = "investor_18427_dayt_cutoff.xlsx", sheetName = "rep", append = TRUE)
write.xlsx(san_18427, file = "investor_18427_dayt_cutoff.xlsx", sheetName = "san", append = TRUE)
write.xlsx(tef_18427, file = "investor_18427_dayt_cutoff.xlsx", sheetName = "tef", append = TRUE)
write.xlsx(zel_18427, file = "investor_18427_dayt_cutoff.xlsx", sheetName = "zel", append = TRUE)
