
// Code generated by stanc v2.24.1
#include <stan/model/model_header.hpp>
namespace binomial_ASER5_state_loo_model_namespace {


inline void validate_positive_index(const char* var_name, const char* expr,
                                    int val) {
  if (val < 1) {
    std::stringstream msg;
    msg << "Found dimension size less than one in simplex declaration"
        << "; variable=" << var_name << "; dimension size expression=" << expr
        << "; expression value=" << val;
    std::string msg_str(msg.str());
    throw std::invalid_argument(msg_str.c_str());
  }
}

inline void validate_unit_vector_index(const char* var_name, const char* expr,
                                       int val) {
  if (val <= 1) {
    std::stringstream msg;
    if (val == 1) {
      msg << "Found dimension size one in unit vector declaration."
          << " One-dimensional unit vector is discrete"
          << " but the target distribution must be continuous."
          << " variable=" << var_name << "; dimension size expression=" << expr;
    } else {
      msg << "Found dimension size less than one in unit vector declaration"
          << "; variable=" << var_name << "; dimension size expression=" << expr
          << "; expression value=" << val;
    }
    std::string msg_str(msg.str());
    throw std::invalid_argument(msg_str.c_str());
  }
}


using std::istream;
using std::string;
using std::stringstream;
using std::vector;
using std::pow;
using stan::io::dump;
using stan::math::lgamma;
using stan::model::model_base_crtp;
using stan::model::rvalue;
using stan::model::cons_list;
using stan::model::index_uni;
using stan::model::index_max;
using stan::model::index_min;
using stan::model::index_min_max;
using stan::model::index_multi;
using stan::model::index_omni;
using stan::model::nil_index_list;
using namespace stan::math;
using stan::math::pow; 

static int current_statement__ = 0;
static const std::vector<string> locations_array__ = {" (found before start of program)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/voterPref/binomial_ASER5_state_loo.stan', line 22, column 0 to column 18)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/voterPref/binomial_ASER5_state_loo.stan', line 23, column 2 to column 28)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/voterPref/binomial_ASER5_state_loo.stan', line 24, column 2 to column 54)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/voterPref/binomial_ASER5_state_loo.stan', line 34, column 0 to column 18)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/voterPref/binomial_ASER5_state_loo.stan', line 36, column 6 to column 119)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/voterPref/binomial_ASER5_state_loo.stan', line 35, column 17 to line 37, column 3)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/voterPref/binomial_ASER5_state_loo.stan', line 35, column 2 to line 37, column 3)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/voterPref/binomial_ASER5_state_loo.stan', line 27, column 0 to column 29)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/voterPref/binomial_ASER5_state_loo.stan', line 28, column 2 to column 45)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/voterPref/binomial_ASER5_state_loo.stan', line 30, column 3 to column 97)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/voterPref/binomial_ASER5_state_loo.stan', line 29, column 17 to line 31, column 3)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/voterPref/binomial_ASER5_state_loo.stan', line 29, column 2 to line 31, column 3)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/voterPref/binomial_ASER5_state_loo.stan', line 2, column 0 to column 17)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/voterPref/binomial_ASER5_state_loo.stan', line 3, column 2 to column 25)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/voterPref/binomial_ASER5_state_loo.stan', line 4, column 2 to column 23)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/voterPref/binomial_ASER5_state_loo.stan', line 5, column 2 to column 23)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/voterPref/binomial_ASER5_state_loo.stan', line 6, column 2 to column 24)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/voterPref/binomial_ASER5_state_loo.stan', line 7, column 2 to column 24)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/voterPref/binomial_ASER5_state_loo.stan', line 12, column 40 to column 41)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/voterPref/binomial_ASER5_state_loo.stan', line 12, column 2 to column 43)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/voterPref/binomial_ASER5_state_loo.stan', line 13, column 67 to column 68)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/voterPref/binomial_ASER5_state_loo.stan', line 13, column 2 to column 70)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/voterPref/binomial_ASER5_state_loo.stan', line 14, column 25 to column 26)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/voterPref/binomial_ASER5_state_loo.stan', line 14, column 2 to column 28)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/voterPref/binomial_ASER5_state_loo.stan', line 15, column 29 to column 30)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/voterPref/binomial_ASER5_state_loo.stan', line 15, column 2 to column 32)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/voterPref/binomial_ASER5_state_loo.stan', line 18, column 0 to column 19)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/voterPref/binomial_ASER5_state_loo.stan', line 19, column 2 to column 42)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/voterPref/binomial_ASER5_state_loo.stan', line 22, column 7 to column 11)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/voterPref/binomial_ASER5_state_loo.stan', line 24, column 33 to column 40)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/voterPref/binomial_ASER5_state_loo.stan', line 24, column 42 to column 46)",
                                                      " (in '/Users/adam/BlueRipple/research/stan/voterPref/binomial_ASER5_state_loo.stan', line 34, column 7 to column 8)"};



class binomial_ASER5_state_loo_model final : public model_base_crtp<binomial_ASER5_state_loo_model> {

 private:
  int G;
  int J_state;
  int J_sex;
  int J_age;
  int J_educ;
  int J_race;
  std::vector<int> state;
  std::vector<int> category;
  std::vector<int> D_votes;
  std::vector<int> Total_votes;
  int nCat;
 
 public:
  ~binomial_ASER5_state_loo_model() final { }
  
  std::string model_name() const final { return "binomial_ASER5_state_loo_model"; }

  std::vector<std::string> model_compile_info() const {
    std::vector<std::string> stanc_info;
    stanc_info.push_back("stanc_version = stanc3 v2.24.1");
    stanc_info.push_back("stancflags = --include-paths=");
    return stanc_info;
  }
  
  
  binomial_ASER5_state_loo_model(stan::io::var_context& context__,
                                 unsigned int random_seed__ = 0,
                                 std::ostream* pstream__ = nullptr) : model_base_crtp(0) {
    using local_scalar_t__ = double ;
    boost::ecuyer1988 base_rng__ = 
        stan::services::util::create_rng(random_seed__, 0);
    (void) base_rng__;  // suppress unused var warning
    static const char* function__ = "binomial_ASER5_state_loo_model_namespace::binomial_ASER5_state_loo_model";
    (void) function__;  // suppress unused var warning
    local_scalar_t__ DUMMY_VAR__(std::numeric_limits<double>::quiet_NaN());
    (void) DUMMY_VAR__;  // suppress unused var warning
    
    try {
      int pos__;
      pos__ = std::numeric_limits<int>::min();
      
      pos__ = 1;
      current_statement__ = 13;
      context__.validate_dims("data initialization","G","int",
          context__.to_vec());
      G = std::numeric_limits<int>::min();
      
      current_statement__ = 13;
      G = context__.vals_i("G")[(1 - 1)];
      current_statement__ = 13;
      current_statement__ = 13;
      check_greater_or_equal(function__, "G", G, 0);
      current_statement__ = 14;
      context__.validate_dims("data initialization","J_state","int",
          context__.to_vec());
      J_state = std::numeric_limits<int>::min();
      
      current_statement__ = 14;
      J_state = context__.vals_i("J_state")[(1 - 1)];
      current_statement__ = 14;
      current_statement__ = 14;
      check_greater_or_equal(function__, "J_state", J_state, 1);
      current_statement__ = 15;
      context__.validate_dims("data initialization","J_sex","int",
          context__.to_vec());
      J_sex = std::numeric_limits<int>::min();
      
      current_statement__ = 15;
      J_sex = context__.vals_i("J_sex")[(1 - 1)];
      current_statement__ = 15;
      current_statement__ = 15;
      check_greater_or_equal(function__, "J_sex", J_sex, 1);
      current_statement__ = 16;
      context__.validate_dims("data initialization","J_age","int",
          context__.to_vec());
      J_age = std::numeric_limits<int>::min();
      
      current_statement__ = 16;
      J_age = context__.vals_i("J_age")[(1 - 1)];
      current_statement__ = 16;
      current_statement__ = 16;
      check_greater_or_equal(function__, "J_age", J_age, 1);
      current_statement__ = 17;
      context__.validate_dims("data initialization","J_educ","int",
          context__.to_vec());
      J_educ = std::numeric_limits<int>::min();
      
      current_statement__ = 17;
      J_educ = context__.vals_i("J_educ")[(1 - 1)];
      current_statement__ = 17;
      current_statement__ = 17;
      check_greater_or_equal(function__, "J_educ", J_educ, 1);
      current_statement__ = 18;
      context__.validate_dims("data initialization","J_race","int",
          context__.to_vec());
      J_race = std::numeric_limits<int>::min();
      
      current_statement__ = 18;
      J_race = context__.vals_i("J_race")[(1 - 1)];
      current_statement__ = 18;
      current_statement__ = 18;
      check_greater_or_equal(function__, "J_race", J_race, 1);
      current_statement__ = 19;
      validate_non_negative_index("state", "G", G);
      current_statement__ = 20;
      context__.validate_dims("data initialization","state","int",
          context__.to_vec(G));
      state = std::vector<int>(G, std::numeric_limits<int>::min());
      
      current_statement__ = 20;
      assign(state, nil_index_list(), context__.vals_i("state"),
        "assigning variable state");
      current_statement__ = 20;
      for (int sym1__ = 1; sym1__ <= G; ++sym1__) {
        current_statement__ = 20;
        current_statement__ = 20;
        check_greater_or_equal(function__, "state[sym1__]",
                               state[(sym1__ - 1)], 1);}
      current_statement__ = 20;
      for (int sym1__ = 1; sym1__ <= G; ++sym1__) {
        current_statement__ = 20;
        current_statement__ = 20;
        check_less_or_equal(function__, "state[sym1__]", state[(sym1__ - 1)],
                            J_state);}
      current_statement__ = 21;
      validate_non_negative_index("category", "G", G);
      current_statement__ = 22;
      context__.validate_dims("data initialization","category","int",
          context__.to_vec(G));
      category = std::vector<int>(G, std::numeric_limits<int>::min());
      
      current_statement__ = 22;
      assign(category, nil_index_list(), context__.vals_i("category"),
        "assigning variable category");
      current_statement__ = 22;
      for (int sym1__ = 1; sym1__ <= G; ++sym1__) {
        current_statement__ = 22;
        current_statement__ = 22;
        check_greater_or_equal(function__, "category[sym1__]",
                               category[(sym1__ - 1)], 1);}
      current_statement__ = 22;
      for (int sym1__ = 1; sym1__ <= G; ++sym1__) {
        current_statement__ = 22;
        current_statement__ = 22;
        check_less_or_equal(function__, "category[sym1__]",
                            category[(sym1__ - 1)],
                            (((J_age * J_sex) * J_educ) * J_race));}
      current_statement__ = 23;
      validate_non_negative_index("D_votes", "G", G);
      current_statement__ = 24;
      context__.validate_dims("data initialization","D_votes","int",
          context__.to_vec(G));
      D_votes = std::vector<int>(G, std::numeric_limits<int>::min());
      
      current_statement__ = 24;
      assign(D_votes, nil_index_list(), context__.vals_i("D_votes"),
        "assigning variable D_votes");
      current_statement__ = 24;
      for (int sym1__ = 1; sym1__ <= G; ++sym1__) {
        current_statement__ = 24;
        current_statement__ = 24;
        check_greater_or_equal(function__, "D_votes[sym1__]",
                               D_votes[(sym1__ - 1)], 0);}
      current_statement__ = 25;
      validate_non_negative_index("Total_votes", "G", G);
      current_statement__ = 26;
      context__.validate_dims("data initialization","Total_votes","int",
          context__.to_vec(G));
      Total_votes = std::vector<int>(G, std::numeric_limits<int>::min());
      
      current_statement__ = 26;
      assign(Total_votes, nil_index_list(), context__.vals_i("Total_votes"),
        "assigning variable Total_votes");
      current_statement__ = 26;
      for (int sym1__ = 1; sym1__ <= G; ++sym1__) {
        current_statement__ = 26;
        current_statement__ = 26;
        check_greater_or_equal(function__, "Total_votes[sym1__]",
                               Total_votes[(sym1__ - 1)], 0);}
      current_statement__ = 27;
      nCat = std::numeric_limits<int>::min();
      
      current_statement__ = 28;
      nCat = (((J_age * J_sex) * J_educ) * J_race);
      current_statement__ = 27;
      current_statement__ = 27;
      check_greater_or_equal(function__, "nCat", nCat, 1);
      current_statement__ = 29;
      validate_non_negative_index("beta", "nCat", nCat);
      current_statement__ = 30;
      validate_non_negative_index("alpha", "J_state", J_state);
      current_statement__ = 31;
      validate_non_negative_index("alpha", "nCat", nCat);
      current_statement__ = 32;
      validate_non_negative_index("log_lik", "G", G);
    } catch (const std::exception& e) {
      stan::lang::rethrow_located(e, locations_array__[current_statement__]);
      // Next line prevents compiler griping about no return
      throw std::runtime_error("*** IF YOU SEE THIS, PLEASE REPORT A BUG ***"); 
    }
    num_params_r__ = 0U;
    
    try {
      num_params_r__ += nCat;
      num_params_r__ += 1;
      num_params_r__ += J_state * nCat;
    } catch (const std::exception& e) {
      stan::lang::rethrow_located(e, locations_array__[current_statement__]);
      // Next line prevents compiler griping about no return
      throw std::runtime_error("*** IF YOU SEE THIS, PLEASE REPORT A BUG ***"); 
    }
  }
  template <bool propto__, bool jacobian__, typename T__>
  inline T__ log_prob(std::vector<T__>& params_r__,
                      std::vector<int>& params_i__,
                      std::ostream* pstream__ = nullptr) const {
    using local_scalar_t__ = T__;
    T__ lp__(0.0);
    stan::math::accumulator<T__> lp_accum__;
    static const char* function__ = "binomial_ASER5_state_loo_model_namespace::log_prob";
(void) function__;  // suppress unused var warning

    stan::io::reader<local_scalar_t__> in__(params_r__, params_i__);
    local_scalar_t__ DUMMY_VAR__(std::numeric_limits<double>::quiet_NaN());
    (void) DUMMY_VAR__;  // suppress unused var warning

    
    try {
      Eigen::Matrix<local_scalar_t__, -1, 1> beta;
      beta = Eigen::Matrix<local_scalar_t__, -1, 1>(nCat);
      stan::math::fill(beta, DUMMY_VAR__);
      
      current_statement__ = 1;
      beta = in__.vector(nCat);
      local_scalar_t__ sigma_alpha;
      sigma_alpha = DUMMY_VAR__;
      
      current_statement__ = 2;
      sigma_alpha = in__.scalar();
      current_statement__ = 2;
      if (jacobian__) {
        current_statement__ = 2;
        sigma_alpha = stan::math::lb_constrain(sigma_alpha, 0, lp__);
      } else {
        current_statement__ = 2;
        sigma_alpha = stan::math::lb_constrain(sigma_alpha, 0);
      }
      Eigen::Matrix<local_scalar_t__, -1, -1> alpha;
      alpha = Eigen::Matrix<local_scalar_t__, -1, -1>(J_state, nCat);
      stan::math::fill(alpha, DUMMY_VAR__);
      
      current_statement__ = 3;
      alpha = in__.matrix(J_state, nCat);
      current_statement__ = 3;
      for (int sym1__ = 1; sym1__ <= J_state; ++sym1__) {
        current_statement__ = 3;
        for (int sym2__ = 1; sym2__ <= nCat; ++sym2__) {
          current_statement__ = 3;
          if (jacobian__) {
            current_statement__ = 3;
            assign(alpha,
              cons_list(index_uni(sym1__),
                cons_list(index_uni(sym2__), nil_index_list())),
              stan::math::offset_multiplier_constrain(
                rvalue(alpha,
                  cons_list(index_uni(sym1__),
                    cons_list(index_uni(sym2__), nil_index_list())), "alpha"),
                0, sigma_alpha, lp__), "assigning variable alpha");
          } else {
            current_statement__ = 3;
            assign(alpha,
              cons_list(index_uni(sym1__),
                cons_list(index_uni(sym2__), nil_index_list())),
              stan::math::offset_multiplier_constrain(
                rvalue(alpha,
                  cons_list(index_uni(sym1__),
                    cons_list(index_uni(sym2__), nil_index_list())), "alpha"),
                0, sigma_alpha), "assigning variable alpha");
          }}}
      {
        current_statement__ = 8;
        lp_accum__.add(normal_lpdf<propto__>(sigma_alpha, 0, 10));
        current_statement__ = 9;
        lp_accum__.add(
          normal_lpdf<propto__>(to_vector(alpha), 0, sigma_alpha));
        current_statement__ = 12;
        for (int g = 1; g <= G; ++g) {
          current_statement__ = 10;
          lp_accum__.add(
            binomial_logit_lpmf<propto__>(D_votes[(g - 1)],
              Total_votes[(g - 1)],
              (beta[(category[(g - 1)] - 1)] +
                rvalue(alpha,
                  cons_list(index_uni(state[(g - 1)]),
                    cons_list(index_uni(category[(g - 1)]), nil_index_list())),
                  "alpha"))));}
      }
    } catch (const std::exception& e) {
      stan::lang::rethrow_located(e, locations_array__[current_statement__]);
      // Next line prevents compiler griping about no return
      throw std::runtime_error("*** IF YOU SEE THIS, PLEASE REPORT A BUG ***"); 
    }
    lp_accum__.add(lp__);
    return lp_accum__.sum();
    } // log_prob() 
    
  template <typename RNG>
  inline void write_array(RNG& base_rng__, std::vector<double>& params_r__,
                          std::vector<int>& params_i__,
                          std::vector<double>& vars__,
                          bool emit_transformed_parameters__ = true,
                          bool emit_generated_quantities__ = true,
                          std::ostream* pstream__ = nullptr) const {
    using local_scalar_t__ = double;
    vars__.resize(0);
    stan::io::reader<local_scalar_t__> in__(params_r__, params_i__);
    static const char* function__ = "binomial_ASER5_state_loo_model_namespace::write_array";
(void) function__;  // suppress unused var warning

    (void) function__;  // suppress unused var warning

    double lp__ = 0.0;
    (void) lp__;  // dummy to suppress unused var warning
    stan::math::accumulator<double> lp_accum__;
    local_scalar_t__ DUMMY_VAR__(std::numeric_limits<double>::quiet_NaN());
    (void) DUMMY_VAR__;  // suppress unused var warning

    
    try {
      Eigen::Matrix<double, -1, 1> beta;
      beta = Eigen::Matrix<double, -1, 1>(nCat);
      stan::math::fill(beta, std::numeric_limits<double>::quiet_NaN());
      
      current_statement__ = 1;
      beta = in__.vector(nCat);
      double sigma_alpha;
      sigma_alpha = std::numeric_limits<double>::quiet_NaN();
      
      current_statement__ = 2;
      sigma_alpha = in__.scalar();
      current_statement__ = 2;
      sigma_alpha = stan::math::lb_constrain(sigma_alpha, 0);
      Eigen::Matrix<double, -1, -1> alpha;
      alpha = Eigen::Matrix<double, -1, -1>(J_state, nCat);
      stan::math::fill(alpha, std::numeric_limits<double>::quiet_NaN());
      
      current_statement__ = 3;
      alpha = in__.matrix(J_state, nCat);
      current_statement__ = 3;
      for (int sym1__ = 1; sym1__ <= J_state; ++sym1__) {
        current_statement__ = 3;
        for (int sym2__ = 1; sym2__ <= nCat; ++sym2__) {
          current_statement__ = 3;
          assign(alpha,
            cons_list(index_uni(sym1__),
              cons_list(index_uni(sym2__), nil_index_list())),
            stan::math::offset_multiplier_constrain(
              rvalue(alpha,
                cons_list(index_uni(sym1__),
                  cons_list(index_uni(sym2__), nil_index_list())), "alpha"),
              0, sigma_alpha), "assigning variable alpha");}}
      for (int sym1__ = 1; sym1__ <= nCat; ++sym1__) {
        vars__.emplace_back(beta[(sym1__ - 1)]);}
      vars__.emplace_back(sigma_alpha);
      for (int sym1__ = 1; sym1__ <= nCat; ++sym1__) {
        for (int sym2__ = 1; sym2__ <= J_state; ++sym2__) {
          vars__.emplace_back(
            rvalue(alpha,
              cons_list(index_uni(sym2__),
                cons_list(index_uni(sym1__), nil_index_list())), "alpha"));}}
      if (logical_negation((primitive_value(emit_transformed_parameters__) ||
            primitive_value(emit_generated_quantities__)))) {
        return ;
      } 
      if (logical_negation(emit_generated_quantities__)) {
        return ;
      } 
      Eigen::Matrix<double, -1, 1> log_lik;
      log_lik = Eigen::Matrix<double, -1, 1>(G);
      stan::math::fill(log_lik, std::numeric_limits<double>::quiet_NaN());
      
      current_statement__ = 7;
      for (int g = 1; g <= G; ++g) {
        current_statement__ = 5;
        assign(log_lik, cons_list(index_uni(g), nil_index_list()),
          binomial_logit_lpmf<false>(D_votes[(g - 1)], Total_votes[(g - 1)],
            (beta[(category[(g - 1)] - 1)] +
              rvalue(alpha,
                cons_list(index_uni(state[(g - 1)]),
                  cons_list(index_uni(category[(g - 1)]), nil_index_list())),
                "alpha"))), "assigning variable log_lik");}
      for (int sym1__ = 1; sym1__ <= G; ++sym1__) {
        vars__.emplace_back(log_lik[(sym1__ - 1)]);}
    } catch (const std::exception& e) {
      stan::lang::rethrow_located(e, locations_array__[current_statement__]);
      // Next line prevents compiler griping about no return
      throw std::runtime_error("*** IF YOU SEE THIS, PLEASE REPORT A BUG ***"); 
    }
    } // write_array() 
    
  inline void transform_inits(const stan::io::var_context& context__,
                              std::vector<int>& params_i__,
                              std::vector<double>& vars__,
                              std::ostream* pstream__) const
    final {
    using local_scalar_t__ = double;
    vars__.clear();
    vars__.reserve(num_params_r__);
    
    try {
      int pos__;
      pos__ = std::numeric_limits<int>::min();
      
      pos__ = 1;
      Eigen::Matrix<double, -1, 1> beta;
      beta = Eigen::Matrix<double, -1, 1>(nCat);
      stan::math::fill(beta, std::numeric_limits<double>::quiet_NaN());
      
      {
        std::vector<local_scalar_t__> beta_flat__;
        current_statement__ = 1;
        assign(beta_flat__, nil_index_list(), context__.vals_r("beta"),
          "assigning variable beta_flat__");
        current_statement__ = 1;
        pos__ = 1;
        current_statement__ = 1;
        for (int sym1__ = 1; sym1__ <= nCat; ++sym1__) {
          current_statement__ = 1;
          assign(beta, cons_list(index_uni(sym1__), nil_index_list()),
            beta_flat__[(pos__ - 1)], "assigning variable beta");
          current_statement__ = 1;
          pos__ = (pos__ + 1);}
      }
      double sigma_alpha;
      sigma_alpha = std::numeric_limits<double>::quiet_NaN();
      
      current_statement__ = 2;
      sigma_alpha = context__.vals_r("sigma_alpha")[(1 - 1)];
      current_statement__ = 2;
      sigma_alpha = stan::math::lb_free(sigma_alpha, 0);
      Eigen::Matrix<double, -1, -1> alpha;
      alpha = Eigen::Matrix<double, -1, -1>(J_state, nCat);
      stan::math::fill(alpha, std::numeric_limits<double>::quiet_NaN());
      
      {
        std::vector<local_scalar_t__> alpha_flat__;
        current_statement__ = 3;
        assign(alpha_flat__, nil_index_list(), context__.vals_r("alpha"),
          "assigning variable alpha_flat__");
        current_statement__ = 3;
        pos__ = 1;
        current_statement__ = 3;
        for (int sym1__ = 1; sym1__ <= nCat; ++sym1__) {
          current_statement__ = 3;
          for (int sym2__ = 1; sym2__ <= J_state; ++sym2__) {
            current_statement__ = 3;
            assign(alpha,
              cons_list(index_uni(sym2__),
                cons_list(index_uni(sym1__), nil_index_list())),
              alpha_flat__[(pos__ - 1)], "assigning variable alpha");
            current_statement__ = 3;
            pos__ = (pos__ + 1);}}
      }
      current_statement__ = 3;
      for (int sym1__ = 1; sym1__ <= J_state; ++sym1__) {
        current_statement__ = 3;
        for (int sym2__ = 1; sym2__ <= nCat; ++sym2__) {
          current_statement__ = 3;
          assign(alpha,
            cons_list(index_uni(sym1__),
              cons_list(index_uni(sym2__), nil_index_list())),
            stan::math::offset_multiplier_free(
              rvalue(alpha,
                cons_list(index_uni(sym1__),
                  cons_list(index_uni(sym2__), nil_index_list())), "alpha"),
              0, sigma_alpha), "assigning variable alpha");}}
      for (int sym1__ = 1; sym1__ <= nCat; ++sym1__) {
        vars__.emplace_back(beta[(sym1__ - 1)]);}
      vars__.emplace_back(sigma_alpha);
      for (int sym1__ = 1; sym1__ <= nCat; ++sym1__) {
        for (int sym2__ = 1; sym2__ <= J_state; ++sym2__) {
          vars__.emplace_back(
            rvalue(alpha,
              cons_list(index_uni(sym2__),
                cons_list(index_uni(sym1__), nil_index_list())), "alpha"));}}
    } catch (const std::exception& e) {
      stan::lang::rethrow_located(e, locations_array__[current_statement__]);
      // Next line prevents compiler griping about no return
      throw std::runtime_error("*** IF YOU SEE THIS, PLEASE REPORT A BUG ***"); 
    }
    } // transform_inits() 
    
  inline void get_param_names(std::vector<std::string>& names__) const {
    
    names__.clear();
    names__.emplace_back("beta");
    names__.emplace_back("sigma_alpha");
    names__.emplace_back("alpha");
    names__.emplace_back("log_lik");
    } // get_param_names() 
    
  inline void get_dims(std::vector<std::vector<size_t>>& dimss__) const
    final {
    dimss__.clear();
    dimss__.emplace_back(std::vector<size_t>{static_cast<size_t>(nCat)});
    
    dimss__.emplace_back(std::vector<size_t>{});
    
    dimss__.emplace_back(std::vector<size_t>{static_cast<size_t>(J_state),
                                             static_cast<size_t>(nCat)});
    
    dimss__.emplace_back(std::vector<size_t>{static_cast<size_t>(G)});
    
    } // get_dims() 
    
  inline void constrained_param_names(
                                      std::vector<std::string>& param_names__,
                                      bool emit_transformed_parameters__ = true,
                                      bool emit_generated_quantities__ = true) const
    final {
    
    for (int sym1__ = 1; sym1__ <= nCat; ++sym1__) {
      {
        param_names__.emplace_back(std::string() + "beta" + '.' + std::to_string(sym1__));
      }}
    param_names__.emplace_back(std::string() + "sigma_alpha");
    for (int sym1__ = 1; sym1__ <= nCat; ++sym1__) {
      {
        for (int sym2__ = 1; sym2__ <= J_state; ++sym2__) {
          {
            param_names__.emplace_back(std::string() + "alpha" + '.' + std::to_string(sym2__) + '.' + std::to_string(sym1__));
          }}
      }}
    if (emit_transformed_parameters__) {
      
    }
    
    if (emit_generated_quantities__) {
      for (int sym1__ = 1; sym1__ <= G; ++sym1__) {
        {
          param_names__.emplace_back(std::string() + "log_lik" + '.' + std::to_string(sym1__));
        }}
    }
    
    } // constrained_param_names() 
    
  inline void unconstrained_param_names(
                                        std::vector<std::string>& param_names__,
                                        bool emit_transformed_parameters__ = true,
                                        bool emit_generated_quantities__ = true) const
    final {
    
    for (int sym1__ = 1; sym1__ <= nCat; ++sym1__) {
      {
        param_names__.emplace_back(std::string() + "beta" + '.' + std::to_string(sym1__));
      }}
    param_names__.emplace_back(std::string() + "sigma_alpha");
    for (int sym1__ = 1; sym1__ <= nCat; ++sym1__) {
      {
        for (int sym2__ = 1; sym2__ <= J_state; ++sym2__) {
          {
            param_names__.emplace_back(std::string() + "alpha" + '.' + std::to_string(sym2__) + '.' + std::to_string(sym1__));
          }}
      }}
    if (emit_transformed_parameters__) {
      
    }
    
    if (emit_generated_quantities__) {
      for (int sym1__ = 1; sym1__ <= G; ++sym1__) {
        {
          param_names__.emplace_back(std::string() + "log_lik" + '.' + std::to_string(sym1__));
        }}
    }
    
    } // unconstrained_param_names() 
    
  inline std::string get_constrained_sizedtypes() const {
    stringstream s__;
    s__ << "[{\"name\":\"beta\",\"type\":{\"name\":\"vector\",\"length\":" << nCat << "},\"block\":\"parameters\"},{\"name\":\"sigma_alpha\",\"type\":{\"name\":\"real\"},\"block\":\"parameters\"},{\"name\":\"alpha\",\"type\":{\"name\":\"matrix\",\"rows\":" << J_state << ",\"cols\":" << nCat << "},\"block\":\"parameters\"},{\"name\":\"log_lik\",\"type\":{\"name\":\"vector\",\"length\":" << G << "},\"block\":\"generated_quantities\"}]";
    return s__.str();
    } // get_constrained_sizedtypes() 
    
  inline std::string get_unconstrained_sizedtypes() const {
    stringstream s__;
    s__ << "[{\"name\":\"beta\",\"type\":{\"name\":\"vector\",\"length\":" << nCat << "},\"block\":\"parameters\"},{\"name\":\"sigma_alpha\",\"type\":{\"name\":\"real\"},\"block\":\"parameters\"},{\"name\":\"alpha\",\"type\":{\"name\":\"matrix\",\"rows\":" << J_state << ",\"cols\":" << nCat << "},\"block\":\"parameters\"},{\"name\":\"log_lik\",\"type\":{\"name\":\"vector\",\"length\":" << G << "},\"block\":\"generated_quantities\"}]";
    return s__.str();
    } // get_unconstrained_sizedtypes() 
    
  
    // Begin method overload boilerplate
    template <typename RNG>
    inline void write_array(RNG& base_rng__,
                     Eigen::Matrix<double,Eigen::Dynamic,1>& params_r,
                     Eigen::Matrix<double,Eigen::Dynamic,1>& vars,
                     bool emit_transformed_parameters__ = true,
                     bool emit_generated_quantities__ = true,
                     std::ostream* pstream = nullptr) const {
      std::vector<double> params_r_vec(params_r.size());
      for (int i = 0; i < params_r.size(); ++i)
        params_r_vec[i] = params_r(i);
      std::vector<double> vars_vec;
      std::vector<int> params_i_vec;
      write_array(base_rng__, params_r_vec, params_i_vec, vars_vec,
          emit_transformed_parameters__, emit_generated_quantities__, pstream);
      vars.resize(vars_vec.size());
      for (int i = 0; i < vars.size(); ++i)
        vars(i) = vars_vec[i];
    }

    template <bool propto__, bool jacobian__, typename T_>
    inline T_ log_prob(Eigen::Matrix<T_,Eigen::Dynamic,1>& params_r,
               std::ostream* pstream = nullptr) const {
      std::vector<T_> vec_params_r;
      vec_params_r.reserve(params_r.size());
      for (int i = 0; i < params_r.size(); ++i)
        vec_params_r.push_back(params_r(i));
      std::vector<int> vec_params_i;
      return log_prob<propto__,jacobian__,T_>(vec_params_r, vec_params_i, pstream);
    }

    inline void transform_inits(const stan::io::var_context& context,
                         Eigen::Matrix<double, Eigen::Dynamic, 1>& params_r,
                         std::ostream* pstream__ = nullptr) const {
      std::vector<double> params_r_vec;
      std::vector<int> params_i_vec;
      transform_inits(context, params_i_vec, params_r_vec, pstream__);
      params_r.resize(params_r_vec.size());
      for (int i = 0; i < params_r.size(); ++i)
        params_r(i) = params_r_vec[i];
    }

};
}

using stan_model = binomial_ASER5_state_loo_model_namespace::binomial_ASER5_state_loo_model;

#ifndef USING_R

// Boilerplate
stan::model::model_base& new_model(
        stan::io::var_context& data_context,
        unsigned int seed,
        std::ostream* msg_stream) {
  stan_model* m = new stan_model(data_context, seed, msg_stream);
  return *m;
}

#endif


