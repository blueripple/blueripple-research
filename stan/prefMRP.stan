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
  int<lower = 0> D_votes[G];
  int<lower = 0> Total_votes[G];
}

parameters {
  real alpha;
  vector[J_sex] bSex;
  vector[J_age] bAge;
  vector[J_educ] bEducation;
  vector[J_race] bRace;
  //  real<lower = 0> sigma_theta;
  //  vector<multiplier = sigma_theta>[J_sex * J_age * J_educ * J_race * J_state] theta;

}

model {
  //  sigma_theta ~ normal (0, 1);
  //  alpha ~ normal (0, 1);
  D_votes ~ binomial_logit(Total_votes
			   , alpha + bSex[sex] + bAge[age] + bEducation[education] + bRace[race]);
}
  
    
