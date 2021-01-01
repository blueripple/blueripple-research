data {
int<lower = 1> N; // number of districts
  int<lower = 1> K; // number of predictors
  int<lower = 1, upper = N> districtE[N]; // do we need this?
  matrix[N, K] Xe;
  int<lower=-1, upper=1> IncE[N];
  int<lower = 0> VAPe[N];
  int<lower = 0> TVotesE[N];
  int<lower = 0> DVotesE[N];
  int<lower = 1> M; // number of cces rows
  int<lower = 1, upper = M> districtC[M]; // do we need this?
  matrix[M, K] Xc;
  int<lower=-1, upper=1> IncC[M];
  int<lower = 0> VAPc[M];
  int<lower = 0> TVotesC[M];
  int<lower = 0> DVotesC[M];
}
transformed data {
int<lower=0> G = M;
  matrix[G, K] X = Xc;
  int<lower=-1, upper=1> Inc[G] = IncC;
  int<lower=0> VAP[G] = VAPc;
  int<lower=0> TVotes[G] = TVotesC;
  int<lower=0> DVotes[G] = DVotesC;vector<lower=0>[K] sigmaPred;
  vector[K] meanPred;
  matrix[G, K] X_centered;
  for (k in 1:K) {
    meanPred[k] = mean(X[,k]);
    X_centered[,k] = X[,k] - meanPred[k];
    sigmaPred[k] = sd(Xe[,k]);
  }
  print("dims(TVotes)=",dims(TVotes));
  print("dims(DVotes)=",dims(DVotes));
  print("dims(X)=",dims(X));
  matrix[G, K] Q_ast;
  matrix[K, K] R_ast;
  matrix[K, K] R_ast_inverse;
  // thin and scale the QR decomposition
  Q_ast = qr_thin_Q(X_centered) * sqrt(G - 1);
  R_ast = qr_thin_R(X_centered) /sqrt(G - 1);
  R_ast_inverse = inverse(R_ast);
}
parameters {
real alphaD;
  vector[K] thetaV;
  real alphaV;
  vector[K] thetaD;
  real incBetaD;
  real <lower=0, upper=1> dispD;
  real <lower=0, upper=1> dispV;
}
transformed parameters {
real<lower=0> phiV = dispV/(1-dispV);
  real<lower=0> phiD = dispD/(1-dispD);
  vector<lower=0, upper=1> [G] pDVoteP = inv_logit (alphaD + Q_ast * thetaD + to_vector(Inc) * incBetaD);
  vector<lower=0, upper=1> [G] pVotedP = inv_logit (alphaV + Q_ast * thetaV);
  vector[K] betaV;
  vector[K] betaD;
  betaV = R_ast_inverse * thetaV;
  betaD = R_ast_inverse * thetaD;
}
model {
alphaD ~ cauchy(0, 10);
  alphaV ~ cauchy(0, 10);
  betaV ~ cauchy(0, 2.5);
  betaD ~ cauchy(0, 2.5);
  incBetaD ~ cauchy(0, 2.5);
  phiD ~ cauchy(0,2);
  phiV ~ cauchy(0,2);
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
  real avgPVoted = inv_logit (alphaV);
  real avgPDVote = inv_logit (alphaD);
  vector[K] sigmaDeltaV;
  vector[K] sigmaDeltaD;
  vector[K] unitDeltaV;
  vector[K] unitDeltaD;
  for (k in 1:K) {
    sigmaDeltaV [k] = inv_logit (alphaV + sigmaPred[k]/2 * betaV[k]) - inv_logit (alphaV - sigmaPred[k]/2 * betaV[k]);
    sigmaDeltaD [k] = inv_logit (alphaD + sigmaPred[k]/2 * betaD[k]) - inv_logit (alphaD - sigmaPred[k]/2 * betaD[k]);
    unitDeltaV[k] = inv_logit (alphaV +  (1-meanPred[k]) * betaV[k]) - inv_logit (alphaV - meanPred[k] * betaV[k]);
    unitDeltaD[k] = inv_logit (alphaD +  (1-meanPred[k]) * betaD[k]) - inv_logit (alphaD - meanPred[k] * betaD[k]);
  }
  real unitDeltaIncD = inv_logit(alphaD + incBetaD) - avgPDVote;
}
