library(rstan)
library(shinystan)
library(loo)
library(bayesplot)
sink(stderr())
print("Loading csv output for stanFit.  Might take a minute or two...")
sink()

stanFit <- read_stan_csv(c("stan/voterPref/output/cces_President_2016_binomial_sepFixedOnly_loo_1.csv","stan/voterPref/output/cces_President_2016_binomial_sepFixedOnly_loo_2.csv","stan/voterPref/output/cces_President_2016_binomial_sepFixedOnly_loo_3.csv","stan/voterPref/output/cces_President_2016_binomial_sepFixedOnly_loo_4.csv"))
sink(stderr())
print("Extracting log likelihood for loo...")
sink()

ll_stanFit <-extract_log_lik(stanFit, merge_chains = FALSE)
sink(stderr())
print("Computing r_eff for loo...")
sink()

re_stanFit <- relative_eff(exp(ll_stanFit), cores = 10)
sink(stderr())
print("Computing loo..")
sink()

cces_President_2016_binomial_sepFixedOnly_loo <- loo(ll_stanFit, r_eff = re_stanFit, cores = 10)
sink(stderr())
print(cces_President_2016_binomial_sepFixedOnly_loo)
sink()

sink(stderr())
print("Computing PSIS...")
sink()
psis_stanFit <- cces_President_2016_binomial_sepFixedOnly_loo$psis_object
sink(stderr())
print("Placing samples insamples_stanFit")
sink()

samples_stanFit <- extract(stanFit)
sink(stderr())
print("E.g., 'ppc_loo_pit_qq(y,as.matrix(samples_stanFit$y_ppred),psis_stanFit$log_weights)'")
sink()

