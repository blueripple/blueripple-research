library(rstan)
library(shinystan)
library(rjson)

setwd("/Users/adam/BlueRipple/research/stan/house/election")
print("Loading csv output.  Might take a minute or two...")
stanFit <- read_stan_csv(c("output/election_betaBinomial_v1_model_1.csv","output/election_betaBinomial_v1_model_2.csv","output/election_betaBinomial_v1_model_3.csv","output/election_betaBinomial_v1_model_4.csv"))
jsonData <- fromJSON(file = "data/election.json")
DVotes <- jsonData $ DVotes
TVotes <- jsonData $ TVotes
print("Launching shinystan....")
launch_shinystan(stanFit)
