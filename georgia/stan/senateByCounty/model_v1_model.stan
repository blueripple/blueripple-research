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
model {
//  alphaD ~ normal(0, 2);
//  alphaV ~ normal(0, 2);
//  betaV ~ normal(0, 5);
//  betaD ~ normal(0, 5);
  TVotes1 ~ binomial_logit(VAP, alphaV + Q_ast * thetaV);
  TVotes2 ~ binomial_logit(VAP, alphaV + Q_ast * thetaV);
  DVotes1 ~ binomial_logit(TVotes1, alphaD + Q_ast * thetaD);
  DVotes2 ~ binomial_logit(TVotes2, alphaD + Q_ast * thetaD);
}
generated quantities {
vector[K] betaV;
  vector[K] betaD;
  betaV = R_ast_inverse * thetaV;
  betaD = R_ast_inverse * thetaD;
  vector<lower = 0, upper = 1>[G] pVotedP;
  vector<lower = 0, upper = 1>[G] pDVoteP;
  pVotedP = inv_logit(alphaV + (Q_ast * thetaV));
  pDVoteP = inv_logit(alphaD + (Q_ast * thetaD));
  vector<lower = 0>[G] eTVotes;
  vector<lower = 0>[G] eDVotes1;
  vector<lower = 0>[G] eDVotes2;
  for (g in 1:G) {
    eTVotes[g] = inv_logit(alphaV + (Q_ast[g] * thetaV)) * VAP[g];
    eDVotes1[g] = inv_logit(alphaD + (Q_ast[g] * thetaD)) * TVotes1[g];
    eDVotes2[g] = inv_logit(alphaD + (Q_ast[g] * thetaD)) * TVotes2[g];
  }
}
