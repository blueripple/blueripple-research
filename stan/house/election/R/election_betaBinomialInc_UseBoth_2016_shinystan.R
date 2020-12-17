library(rstan)
library(shinystan)
library(rjson)

setwd("/Users/adam/BlueRipple/research/stan/house/election")
print("Loading csv output.  Might take a minute or two...")
stanFit <- read_stan_csv(c("output/election_betaBinomialInc_UseBoth_2016_1.csv","output/election_betaBinomialInc_UseBoth_2016_2.csv","output/election_betaBinomialInc_UseBoth_2016_3.csv","output/election_betaBinomialInc_UseBoth_2016_4.csv"))
jsonData <- fromJSON(file = "data/election_2016.json")
DVotes <- append (jsonData $ DVotesE, jsonData $ DVotesC
TVotes <- append (jsonData $ TVotesE, jsonData $ TVotesC
print("Launching shinystan....")
launch_shinystan(stanFit)
