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
  mutate(intraday_change = Close - Open, ibex_percent_change = (intraday_change / Close))
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

# Count gains and losses for IBEX35
ibex_count_gain <- ibex_prices %>%
  filter(ibex_percent_change > 0) %>%
  summarize(count_gain = n())
ibex_gain_total <- ibex_count_gain[[1]]
ibex_count_loss <- ibex_prices %>%
  filter(ibex_percent_change < 0) %>%
  summarize(count_loss = n())
ibex_loss_total <- ibex_count_loss[[1]]

# Create one large investors data frame and split correlated\uncorrelated data frames
investors <- do.call("rbind", list(bbva_investors, ele_investors, eze_investors, gas_investors, rep_investors, san_investors, tef_investors, zel_investors))
investors_correlated <- do.call("rbind", list(bbva_investors, ele_investors, gas_investors, rep_investors, san_investors))
investors_uncorrelated <- do.call("rbind", list(eze_investors, tef_investors, zel_investors))

# Calculate total transactions per investor
total_transactions <- investors %>%
  group_by(investor_id) %>%
  filter(daily_position_change != 0) %>%
  summarize(total_transactions = n())
# Calculate total transactions after gain in IBEX35
total_transactions_gain <- investors %>%
  group_by(investor_id) %>%
  filter(daily_position_change != 0 & ibex_percent_change > 0) %>%
  summarize(total_transactions_gain = n())
# Calculate total transactions after drop in IBEX35
total_transactions_loss <- investors %>%
  group_by(investor_id) %>%
  filter(daily_position_change != 0 & ibex_percent_change < 0) %>%
  summarize(total_transactions_loss = n())

# Add binary bought/sold columns
investors <- investors %>%
  mutate(bought = if_else(daily_position_change > 0, 1, 0),
         sold = if_else(daily_position_change < 0, 1, 0))

# Calculate proportion of buying following gain in IBEX35
investors_buy_gain <- investors %>%
  group_by(investor_id) %>%
  filter(daily_position_change > 0, ibex_percent_change > 0) %>%
  summarize(buy_gain = n())
investors_buy_gain <- investors_buy_gain %>%
  mutate(proportion = buy_gain / ibex_gain_total)
# Calculate proportion of selling following drop in IBEX35
investors_sell_loss <- investors %>%
  group_by(investor_id) %>%
  filter(daily_position_change < 0, ibex_percent_change < 0) %>%
  summarize(sell_loss = n())
investors_sell_loss <- investors_sell_loss %>%
  mutate(proportion = sell_loss / ibex_loss_total)
# Calculate proportion of buying following drop in IBEX35
investors_buy_loss <- investors %>%
  group_by(investor_id) %>%
  filter(daily_position_change > 0, ibex_percent_change < 0) %>%
  summarize(buy_loss = n())
investors_buy_loss <- investors_buy_loss %>%
  mutate(proportion = buy_loss / ibex_loss_total)
# Calculate proportion of selling following gain in IBEX35
investors_sell_gain <- investors %>%
  group_by(investor_id) %>%
  filter(daily_position_change < 0, ibex_percent_change > 0) %>%
  summarize(sell_gain = n())
investors_sell_gain <- investors_sell_gain %>%
  mutate(proportion = sell_gain / ibex_gain_total)

# Same as above split by correlated stocks
# Calculate proportion of buying following gain in IBEX35
correlated_buy_gain <- investors_correlated %>%
  filter(daily_position_change > 0, ibex_percent_change > 0) %>%
  group_by(investor_id) %>%
  summarize(buy_gain = n())
correlated_buy_gain <- correlated_buy_gain %>%
  mutate(proportion = buy_gain / ibex_gain_total)
# Add total transactions per investor column
correlated_buy_gain <- correlated_buy_gain %>%
  inner_join(total_transactions, by = "investor_id") %>%
  inner_join(total_transactions_gain, by = "investor_id")
# Calculate proportion of selling following drop in IBEX35
correlated_sell_loss <- investors_correlated %>%
  filter(daily_position_change < 0, ibex_percent_change < 0) %>%
  group_by(investor_id) %>%
  summarize(sell_loss = n())
correlated_sell_loss <- correlated_sell_loss %>%
  mutate(proportion = sell_loss / ibex_loss_total)
# Add total transactions per investor column
correlated_sell_loss <- correlated_sell_loss %>%
  inner_join(total_transactions, by = "investor_id") %>%
  inner_join(total_transactions_loss, by = "investor_id")
# Calculate proportion of buying following drop in IBEX35
correlated_buy_loss <- investors_correlated %>%
  filter(daily_position_change > 0, ibex_percent_change < 0) %>%
  group_by(investor_id) %>%
  summarize(buy_loss = n())
correlated_buy_loss <- correlated_buy_loss %>%
  mutate(proportion = buy_loss / ibex_loss_total)
# Add total transactions per investor column
correlated_buy_loss <- correlated_buy_loss %>%
  inner_join(total_transactions, by = "investor_id") %>%
  inner_join(total_transactions_loss, by = "investor_id")
# Calculate proportion of selling following gain in IBEX35
correlated_sell_gain <- investors_correlated %>%
  filter(daily_position_change < 0, ibex_percent_change > 0) %>%
  group_by(investor_id) %>%
  summarize(sell_gain = n())
correlated_sell_gain <- correlated_sell_gain %>%
  mutate(proportion = sell_gain / ibex_gain_total)
# Add total transactions per investor column
correlated_sell_gain <- correlated_sell_gain %>%
  inner_join(total_transactions, by = "investor_id") %>%
  inner_join(total_transactions_gain, by = "investor_id")

# Same as above split by uncorrelated stocks
# Calculate proportion of buying following gain in IBEX35
uncorrelated_buy_gain <- investors_uncorrelated %>%
  filter(daily_position_change > 0, ibex_percent_change > 0) %>%
  group_by(investor_id) %>%
  summarize(buy_gain = n())
uncorrelated_buy_gain <- uncorrelated_buy_gain %>%
  mutate(proportion = buy_gain / ibex_gain_total)
# Add total transactions per investor column
uncorrelated_buy_gain <- uncorrelated_buy_gain %>%
  inner_join(total_transactions, by = "investor_id") %>%
  inner_join(total_transactions_gain, by = "investor_id")
# Calculate proportion of selling following drop in IBEX35
uncorrelated_sell_loss <- investors_uncorrelated %>%
  filter(daily_position_change < 0, ibex_percent_change < 0) %>%
  group_by(investor_id) %>%
  summarize(sell_loss = n())
uncorrelated_sell_loss <- uncorrelated_sell_loss %>%
  mutate(proportion = sell_loss / ibex_loss_total)
# Add total transactions per investor column
uncorrelated_sell_loss <- uncorrelated_sell_loss %>%
  inner_join(total_transactions, by = "investor_id") %>%
  inner_join(total_transactions_loss, by = "investor_id")
# Calculate proportion of buying following drop in IBEX35
uncorrelated_buy_loss <- investors_uncorrelated %>%
  filter(daily_position_change > 0, ibex_percent_change < 0) %>%
  group_by(investor_id) %>%
  summarize(buy_loss = n())
uncorrelated_buy_loss <- uncorrelated_buy_loss %>%
  mutate(proportion = buy_loss / ibex_loss_total)
# Add total transactions per investor column
uncorrelated_buy_loss <- uncorrelated_buy_loss %>%
  inner_join(total_transactions, by = "investor_id") %>%
  inner_join(total_transactions_loss, by = "investor_id")
# Calculate proportion of selling following gain in IBEX35
uncorrelated_sell_gain <- investors_uncorrelated %>%
  filter(daily_position_change < 0, ibex_percent_change > 0) %>%
  group_by(investor_id) %>%
  summarize(sell_gain = n())
uncorrelated_sell_gain <- uncorrelated_sell_gain %>%
  mutate(proportion = sell_gain / ibex_gain_total)
# Add total transactions per investor column
uncorrelated_sell_gain <- uncorrelated_sell_gain %>%
  inner_join(total_transactions, by = "investor_id") %>%
  inner_join(total_transactions_gain, by = "investor_id")

# Add column for percent out of total transactions and rename proportion column
correlated_buy_gain <- correlated_buy_gain %>%
  mutate(percent_of_total_transactions = buy_gain / total_transactions) %>%
  mutate(percent_of_transactions_after_gain = buy_gain / total_transactions_gain) %>%
  rename(percent_of_ibex_gains = proportion)
correlated_buy_loss <- correlated_buy_loss %>%
  mutate(percent_of_total_transactions = buy_loss / total_transactions) %>%
  mutate(percent_of_transactions_after_loss = buy_loss / total_transactions_loss) %>%
  rename(percent_of_ibex_losses = proportion)
correlated_sell_gain <- correlated_sell_gain %>%
  mutate(percent_of_total_transactions = sell_gain / total_transactions) %>%
  mutate(percent_of_transactions_after_gain = sell_gain / total_transactions_gain) %>%
  rename(percent_of_ibex_gains = proportion)
correlated_sell_loss <- correlated_sell_loss %>%
  mutate(percent_of_total_transactions = sell_loss / total_transactions) %>%
  mutate(percent_of_transactions_after_loss = sell_loss / total_transactions_loss) %>%
  rename(percent_of_ibex_losses = proportion)
uncorrelated_buy_gain <- uncorrelated_buy_gain %>%
  mutate(percent_of_total_transactions = buy_gain / total_transactions) %>%
  mutate(percent_of_transactions_after_gain = buy_gain / total_transactions_gain) %>%
  rename(percent_of_ibex_gains = proportion)
uncorrelated_buy_loss <- uncorrelated_buy_loss %>%
  mutate(percent_of_total_transactions = buy_loss / total_transactions) %>%
  mutate(percent_of_transactions_after_loss = buy_loss / total_transactions_loss) %>%
  rename(percent_of_ibex_losses = proportion)
uncorrelated_sell_gain <- uncorrelated_sell_gain %>%
  mutate(percent_of_total_transactions = sell_gain / total_transactions) %>%
  mutate(percent_of_transactions_after_gain = sell_gain / total_transactions_gain) %>%
  rename(percent_of_ibex_gains = proportion)
uncorrelated_sell_loss <- uncorrelated_sell_loss %>%
  mutate(percent_of_total_transactions = sell_loss / total_transactions) %>%
  mutate(percent_of_transactions_after_loss = sell_loss / total_transactions_loss) %>%
  rename(percent_of_ibex_losses = proportion)

# Create list of all individual investors in the correlated stocks
investors_list_correlated <- investors_correlated %>% select(investor_id)
investors_list_correlated <- unique(investors_list_correlated)
# Same as above for the uncorrelated stocks
investors_list_uncorrelated <- investors_uncorrelated %>% select(investor_id)
investors_list_uncorrelated <- unique(investors_list_uncorrelated)

# Add missing investor id
correlated_buy_gain <- correlated_buy_gain %>%
  right_join(investors_list_correlated) %>%
  arrange(investor_id)
correlated_buy_loss <- correlated_buy_loss %>%
  right_join(investors_list_correlated) %>%
  arrange(investor_id)
correlated_sell_gain <- correlated_sell_gain %>%
  right_join(investors_list_correlated) %>%
  arrange(investor_id)
correlated_sell_loss <- correlated_sell_loss %>%
  right_join(investors_list_correlated) %>%
  arrange(investor_id)
uncorrelated_buy_gain <- uncorrelated_buy_gain %>%
  right_join(investors_list_uncorrelated) %>%
  arrange(investor_id)
uncorrelated_buy_loss <- uncorrelated_buy_loss %>%
  right_join(investors_list_uncorrelated) %>%
  arrange(investor_id)
uncorrelated_sell_gain <- uncorrelated_sell_gain %>%
  right_join(investors_list_uncorrelated) %>%
  arrange(investor_id)
uncorrelated_sell_loss <- uncorrelated_sell_loss %>%
  right_join(investors_list_uncorrelated) %>%
  arrange(investor_id)

# Check results
mean(correlated_buy_gain$percent_of_ibex_gains, na.rm = TRUE)
mean(correlated_buy_loss$percent_of_ibex_losses, na.rm = TRUE)
mean(correlated_sell_gain$percent_of_ibex_gains, na.rm = TRUE)
mean(correlated_sell_loss$percent_of_ibex_losses, na.rm = TRUE)
mean(uncorrelated_buy_gain$percent_of_ibex_gains, na.rm = TRUE)
mean(uncorrelated_buy_loss$percent_of_ibex_losses, na.rm = TRUE)
mean(uncorrelated_sell_gain$percent_of_ibex_gains, na.rm = TRUE)
mean(uncorrelated_sell_loss$percent_of_ibex_losses, na.rm = TRUE)

# Export
write.csv(correlated_buy_gain, file = "correlated_buy_gain2.csv")
write.csv(correlated_buy_loss, file = "correlated_buy_loss2.csv")
write.csv(correlated_sell_gain, file = "correlated_sell_gain2.csv")
write.csv(correlated_sell_loss, file = "correlated_sell_loss2.csv")
write.csv(uncorrelated_buy_gain, file = "uncorrelated_buy_gain2.csv")
write.csv(uncorrelated_buy_loss, file = "uncorrelated_buy_loss2.csv")
write.csv(uncorrelated_sell_gain, file = "uncorrelated_sell_gain2.csv")
write.csv(uncorrelated_sell_loss, file = "uncorrelated_sell_loss2.csv")