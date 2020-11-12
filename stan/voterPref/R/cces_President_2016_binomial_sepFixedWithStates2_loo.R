library(rstan)
library(shinystan)
library(loo)
setwd("/Users/adam/BlueRipple/research/stan/voterPref")
print("Loading csv output.  Might take a minute or two...")
stanFit <- read_stan_csv(c("output/cces_President_2016_binomial_sepFixedWithStates2_loo_1.csv","output/cces_President_2016_binomial_sepFixedWithStates2_loo_2.csv","output/cces_President_2016_binomial_sepFixedWithStates2_loo_3.csv","output/cces_President_2016_binomial_sepFixedWithStates2_loo_4.csv"))
print("Extracting log likelihood for loo...")
log_lik <-extract_log_lik(stanFit, merge_chains = FALSE)
print("Computing r_eff for loo...")
rel_eff <- relative_eff(exp(log_lik), cores = 10)
print("Computing loo..")
cces_President_2016_binomial_sepFixedWithStates2_loo <- loo(log_lik, r_eff = rel_eff, cores = 10)
print(cces_President_2016_binomial_sepFixedWithStates2_loo)