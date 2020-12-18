library(rstan)
library(shinystan)
library(rjson)

setwd("/Users/adam/BlueRipple/research/stan/house/election")
print("Loading csv output.  Might take a minute or two...")
stanFit <- read_stan_csv(c("output/election_betaBinomialInc_UseElectionResults_2012_1.csv","output/election_betaBinomialInc_UseElectionResults_2012_2.csv","output/election_betaBinomialInc_UseElectionResults_2012_3.csv","output/election_betaBinomialInc_UseElectionResults_2012_4.csv"))
jsonData <- fromJSON(file = "data/election_2012.json")
DVotes <- jsonData $ DVotesE
TVotes <- jsonData $ TVotesE
print("Launching shinystan....")
launch_shinystan(stanFit)
