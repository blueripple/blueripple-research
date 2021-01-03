library(rstan)
library(shinystan)
library(rjson)

setwd("/Users/adam/BlueRipple/research/stan/house/election")
print("Loading csv output.  Might take a minute or two...")
stanFit <- read_stan_csv(c("output/election_betaBinomialInc_UseCCES_2012_1.csv","output/election_betaBinomialInc_UseCCES_2012_2.csv","output/election_betaBinomialInc_UseCCES_2012_3.csv","output/election_betaBinomialInc_UseCCES_2012_4.csv"))
jsonData <- fromJSON(file = "data/election_UseCCES_2012.json")
DVotes <- jsonData $ DVotesC
TVotes <- jsonData $ TVotesC
print("Launching shinystan....")
launch_shinystan(stanFit)
