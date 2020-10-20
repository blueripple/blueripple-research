data {
int<lower = 0> G; // number of cells
  int<lower = 1> J_state; // number of states
  int<lower = 1> J_sex; // number of sex categories
  int<lower = 1> J_age; // number of age categories
  int<lower = 1> J_educ; // number of education categories
  int<lower = 1> J_race; // number of race categories  
  int<lower = 1, upper = J_sex> sex[G];
  int<lower = 1, upper = J_age> age[G];
  int<lower = 1, upper = J_educ> education[G];
  int<lower = 1, upper = J_race> race[G];
  int<lower = 1, upper = J_state> state[G];
  int<lower = 1, upper = J_age * J_sex * J_educ * J_race> category[G];
  int<lower = 0> D_votes[G];
  int<lower = 0> Total_votes[G];
}
parameters {
vector[J_sex] alpha_sex;
  vector[J_age] alpha_age;
  vector[J_educ] alpha_education;
  vector[J_race] alpha_race;
}
model {
alpha_sex[1] ~ normal(0,10)
  alpha_sex[2] ~ normal(0,10)
  alpha_age[1] ~ normal(0,10)
  alpha_education[2] ~ normal(0,10)
  alpha_education[2] ~ normal(0,10)
  alpha_race[1] ~ normal(0,10)
  alpha_race[2] ~ normal(0,10)
  alpha_race[3] ~ normal(0,10)
  alpha_race[4] ~ normal(0,10)
  alpha_race[5] ~ normal(0,10)
  for (g in 1:G) {
    D_votes[g] ~ binomial_logit(Total_votes[g], alpha_sex[sex[g]] + alpha_age[age[g]] + alpha_education[education[g]] + alpha_race[race[g]]);
  }
}
generated quantities {
vector[G] log_lik;
  for (g in 1:G) {
    log_lik[g] =  binomial_logit_lpmf(D_votes[g] | Total_votes[g], alpha_sex[sex[g]] + alpha_age[age[g]] + alpha_education[education[g]] + alpha_race[race[g]]);
  }
}
