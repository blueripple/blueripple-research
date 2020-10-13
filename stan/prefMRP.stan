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
  //  int<lower = 1, upper = J_state> state[G];
  int<lower = 1, upper = J_age * J_sex * J_educ * J_race> category[G];
  int<lower = 0> D_votes[G];
  int<lower = 0> Total_votes[G];
}

parameters {
  //  real alpha;
  vector[J_sex * J_age * J_educ * J_race] beta;
  //  vector[J_sex] bSex;
  //  vector[J_age] bAge;
  //  vector[J_educ] bEducation;
  //  vector[J_race] bRace;
  //  real<lower = 0> sigma_theta;
  //  vector<multiplier = sigma_theta>[J_sex * J_age * J_educ * J_race * J_state] theta;
}
/*
transformed parameters {
  real alpha_p;
  alpha_p = inv_logit (alpha);
}
*/
model {
  //  sigma_theta ~ normal (0, 1);
  //  alpha_p ~ normal (0.5, 1);
  D_votes ~ binomial_logit(Total_votes, beta[category]); //bSex[sex] + bAge[age] + bEducation[education] + bRace[race]);
}
  
    
generated quantities {
  vector <lower = 0, upper = 1>[J_age * J_sex * J_educ * J_race] probs;
  probs = inv_logit(beta[category]);
    
}
