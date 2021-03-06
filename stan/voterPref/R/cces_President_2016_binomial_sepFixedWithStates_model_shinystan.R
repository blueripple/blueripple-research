library(rstan)
library(shinystan)
library(rjson)

sink(stderr())
print("Loading csv output.  Might take a minute or two...")
sink()

stanFit <- read_stan_csv(c("stan/voterPref/output/cces_President_2016_binomial_sepFixedWithStates_model_1.csv","stan/voterPref/output/cces_President_2016_binomial_sepFixedWithStates_model_2.csv","stan/voterPref/output/cces_President_2016_binomial_sepFixedWithStates_model_3.csv","stan/voterPref/output/cces_President_2016_binomial_sepFixedWithStates_model_4.csv"))
jsonData <- fromJSON(file = "stan/voterPref/data/cces_President_2016_v2.json")
D_votes <- jsonData $ D_votes
sink(stderr())
print("Launching shinystan....")
sink()

launch_shinystan(stanFit)
