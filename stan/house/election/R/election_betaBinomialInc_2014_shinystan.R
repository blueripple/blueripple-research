library(rstan)
library(shinystan)
library(rjson)

setwd("/Users/adam/BlueRipple/research/stan/house/election")
print("Loading csv output.  Might take a minute or two...")
stanFit <- read_stan_csv(c("output/election_betaBinomialInc_2014_1.csv","output/election_betaBinomialInc_2014_2.csv","output/election_betaBinomialInc_2014_3.csv","output/election_betaBinomialInc_2014_4.csv"))
jsonData <- fromJSON(file = "data/election_2014.json")
DVotes <- jsonData $ DVotes
TVotes <- jsonData $ TVotes
print("Launching shinystan....")
launch_shinystan(stanFit)
