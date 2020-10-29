library(rstan)
library(shinystan)

setwd("/Users/adam/BlueRipple/research/stan/voterPref")
print("Loading csv output.  Might take a minute or two...")
stanFit <- read_stan_csv(c("output/cces_President_2016_binomial_sepFixedWithStates3_model_1.csv","output/cces_President_2016_binomial_sepFixedWithStates3_model_2.csv","output/cces_President_2016_binomial_sepFixedWithStates3_model_3.csv","output/cces_President_2016_binomial_sepFixedWithStates3_model_4.csv"))
print("Launching shinystan....")
launch_shinystan(stanFit)
