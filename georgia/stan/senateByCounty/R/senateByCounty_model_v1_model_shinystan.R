library(rstan)
library(shinystan)

setwd("/Users/adam/BlueRipple/research/georgia/stan/senateByCounty")
print("Loading csv output.  Might take a minute or two...")
stanFit <- read_stan_csv(c("output/senateByCounty_model_v1_model_1.csv","output/senateByCounty_model_v1_model_2.csv","output/senateByCounty_model_v1_model_3.csv","output/senateByCounty_model_v1_model_4.csv"))
print("Launching shinystan....")
launch_shinystan(stanFit)
