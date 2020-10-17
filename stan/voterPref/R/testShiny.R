library(rstan)
library(shinystan)
library(bayesplot)
library(loo)

options(mc.cores = 10)

setwd("~/BlueRipple/research/stan/voterPref")

stanFit <- read_stan_csv(c("output/cces_President_2016_binomial_ASER5_state_model_1.csv","output/cces_President_2016_binomial_ASER5_state_model_2.csv"))

stanFit@stanmodel <- stan_model("binomial_ASER5_model.stan")




