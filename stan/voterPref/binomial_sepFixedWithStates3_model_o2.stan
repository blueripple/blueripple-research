data {
int<lower = 0> G; // number of cells
  int<lower = 1> J_state; // number of states
  int<lower = 1, upper = J_state> state[G];
  int<lower = 1> K; // number of cols in predictor matrix
  matrix[G, K] X;
  int<lower = 0> D_votes[G];
  int<lower = 0> Total_votes[G];
  int<lower = 0> M;
  int<lower = 0> predict_State[M];
  matrix[M, K] predict_X;
}
transformed data {
vector[G] intcpt;
  vector[M] predictIntcpt;
  matrix[G, K+1] XI; // add the intercept so the covariance matrix is easier to deal with
  matrix[M, K+1] predict_XI;
  for (g in 1:G)
    intcpt[g] = 1;
  XI = append_col(intcpt, X);
  for (m in 1:M)
    predictIntcpt[m] = 1;
  predict_XI = append_col(predictIntcpt, predict_X);
}
parameters {
real alpha; // overall intercept
  vector[K] beta; // fixed effects
  vector<lower=0> [K+1] tau; // group effects scales
  corr_matrix[K+1] Omega; // group effect correlations
  vector[K+1] betaState[J_state]; // state-level coefficients
}
model {
alpha ~ normal(0,2); // weak prior around 50%
  beta ~ normal(0,1);
  tau ~ cauchy(0, 2.5);
  Omega ~ lkj_corr(2);
  for (s in 1:J_state) {
    vector[K] zero;
    for (k in 1:(K+1))
      zero[k] = 0;
    betaState[s] ~ multi_normal(zero, quad_form_diag(Omega, tau));
  }
  {
    vector[G] xiBetaState;
    for (g in 1:G)
      xiBetaState[g] = XI[g] * betaState[state[g]];
    D_votes ~ binomial_logit(Total_votes, alpha + (X * beta) + xiBetaState);
  }
}
generated quantities {
vector<lower = 0, upper = 1>[M] predicted;
  for (m in 1:M)
    predicted[m] = inv_logit(alpha + (predict_X[m] * beta) + (predict_XI[m] * betaState[predict_State[m]]));
}
