data {
int<lower = 1> G; // number of counties
  int<lower = 1> K; // number of predictors
  int<lower = 1, upper = G> county[G]; // do we need this?
  matrix[G, K] X;
  int<lower = 0> VAP[G];
  int<lower = 0> DVotes1[G];
  int<lower = 0> DVotes2[G];
  int<lower = 0> TVotes1[G];
  int<lower = 0> TVotes2[G];
}
parameters {
real alphaD;                             
  vector[K] betaV;
  real alphaV;
  vector[K] betaD;
}
model {
alphaD ~ normal(0, 2);
  alphaV ~ normal(0, 2);
  betaV ~ normal(0, 1);
  betaD ~ normal(0, 1);
  TVotes1 ~ binomial_logit(VAP, alphaV + X * betaV);
//  TVotes2 ~ binomial_logit(VAP, alphaV + X * betaV);
  DVotes1 ~ binomial_logit(TVotes1, alphaD + X * betaD);
//  DVotes2 ~ binomial_logit(TVotes2, alphaD + X * betaD);
}
generated quantities {
vector<lower = 0, upper = 1>[G] pVotedP;
  vector<lower = 0, upper = 1>[G] pDVoteP;
  pVotedP = inv_logit(alphaV + (X * betaV));
  pDVoteP = inv_logit(alphaD + (X * betaD));
}
