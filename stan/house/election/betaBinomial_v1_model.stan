data {
int<lower = 1> G; // number of counties
  int<lower = 1> K; // number of predictors
  int<lower = 1, upper = G> district[G]; // do we need this?
  matrix[G, K] X;
  int<lower = 0> VAP[G];
  int<lower = 0> TVotes[G];
  int<lower = 0> DVotes[G];
}
transformed data {
matrix[G, K] Q_ast;
  matrix[K, K] R_ast;
  matrix[K, K] R_ast_inverse;
  // thin and scale the QR decomposition
  Q_ast = qr_Q(X)[, 1:K] * sqrt(G - 1);
  R_ast = qr_R(X)[1:K,]/sqrt(G - 1);
  R_ast_inverse = inverse(R_ast);
}
parameters {
real alphaD;
  real <lower=0, upper=1> dispD;                             
  vector[K] thetaV;
  real alphaV;
  real <lower=0, upper=1> dispV;
  vector[K] thetaD;
}
transformed parameters {
real <lower=0> phiD = (1-dispD)/dispD;
  real <lower=0> phiV = (1-dispV)/dispV;
  vector [G] pDVoteP = inv_logit (alphaD + Q_ast * thetaD);
  vector [G] pVotedP = inv_logit (alphaV + Q_ast * thetaV);
  vector [K] betaV;
  vector [K] betaD;
  betaV = R_ast_inverse * thetaV;
  betaD = R_ast_inverse * thetaD;
}
model {
alphaD ~ cauchy(0, 10);
  alphaV ~ cauchy(0, 10);
  betaV ~ cauchy(0, 2.5);
  betaD ~ cauchy(0, 2.5);

  TVotes ~ beta_binomial(VAP, pVotedP * phiV, (1 - pVotedP) * phiV);
  DVotes ~ beta_binomial(TVotes, pDVoteP * phiD, (1 - pDVoteP) * phiD);
}
generated quantities {
vector[G] log_lik;
  for (g in 1:G) {
    log_lik[g] =  beta_binomial_lpmf(DVotes[g] | TVotes[g], pDVoteP[g] * phiD, (1 - pDVoteP[g]) * phiD) ;
  }
vector<lower = 0>[G] eTVotes;
  vector<lower = 0>[G] eDVotes;
  for (g in 1:G) {
    eTVotes[g] = pVotedP[g] * VAP[g];
    eDVotes[g] = pDVoteP[g] * TVotes[g];
  }
}
