data {
int<lower = 0> G; // number of cells
  int<lower = 1> J_state; // number of states
  int<lower = 1> J_sex; // number of sex categories
  int<lower = 1> J_age; // number of age categories
  int<lower = 1> J_educ; // number of education categories
  int<lower = 1> J_race; // number of race categories  
  //  int<lower = 1, upper = J_sex> sex[G];
  //  int<lower = 1, upper = J_age> age[G];
  //  int<lower = 1, upper = J_educ> education[G];
  //  int<lower = 1, upper = J_race> race[G];
  int<lower = 1, upper = J_state> state[G];
  int<lower = 1, upper = J_age * J_sex * J_educ * J_race> category[G];
  int<lower = 0> D_votes[G];
  int<lower = 0> Total_votes[G];
}
transformed data {
int <lower=1> nCat;
  nCat =  J_age * J_sex * J_educ * J_race;
}
parameters {
vector[nCat] beta;
  real<lower=0> sigma_alpha;
  vector<multiplier=sigma_alpha>[J_state] alpha;
}
model {
sigma_alpha ~ normal (0, 10);
  alpha ~ normal (0, sigma_alpha);
  D_votes ~ binomial_logit(Total_votes, beta[category] + alpha[state]);
}
generated quantities {
vector[G] log_lik;
  for (g in 1:G) {
    log_lik[g] =  binomial_logit_lpmf(D_votes[g] | Total_votes[g], beta[category[g]] + alpha[state[g]]);
  }
}
