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
  vector[K] thetaV;
  real alphaV;
  vector[K] thetaD;
}
transformed parameters {
vector [K] betaV;
  vector [K] betaD;
  betaV = R_ast_inverse * thetaV;
  betaD = R_ast_inverse * thetaD;
}
model {
alphaD ~ cauchy(0, 10);
  alphaV ~ cauchy(0, 10);
  betaD ~ cauchy(0, 2.5);
  betaV ~ cauchy(0,2.5);
  TVotes ~ binomial_logit(VAP, alphaV + Q_ast * thetaV);
  DVotes ~ binomial_logit(TVotes, alphaD + Q_ast * thetaD);
}
generated quantities {
vector[G] log_lik;
//  log_lik = binomial_logit_lpmf(DVotes1 | TVotes1, alphaD + Q_ast * thetaD);
  for (g in 1:G) {
    log_lik[g] =  binomial_logit_lpmf(DVotes[g] | TVotes[g], alphaD + Q_ast[g] * thetaD);
  }
vector<lower = 0, upper = 1>[G] pVotedP;
  vector<lower = 0, upper = 1>[G] pDVoteP;
  pVotedP = inv_logit(alphaV + (Q_ast * thetaV));
  pDVoteP = inv_logit(alphaD + (Q_ast * thetaD));
  vector<lower = 0>[G] eTVotes;
  vector<lower = 0>[G] eDVotes;
  for (g in 1:G) {
    eTVotes[g] = inv_logit(alphaV + (Q_ast[g] * thetaV)) * VAP[g];
    eDVotes[g] = inv_logit(alphaD + (Q_ast[g] * thetaD)) * TVotes[g];
  }
}
