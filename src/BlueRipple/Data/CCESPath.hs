{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
module BlueRipple.Data.CCESPath where

import qualified Frames.Streamly.TH                     as FS
import qualified Frames.Streamly.TH as FS

import qualified Data.Set as S
import qualified Data.Map as M
--import BlueRipple.Data.Loaders (presidentialElectionsWithIncumbency, presidentialByStateFrame)


dataDir :: FilePath
dataDir = "../bigData/CCES/"

cces2018C_CSV :: FilePath = dataDir ++ "CCES_cumulative_2006_2018.csv"
cces2020C_CSV :: FilePath = dataDir ++ "CES_cumulative_2006-2020.csv"

ccesCols2018C :: S.Set FS.HeaderText
ccesCols2018C = S.fromList (FS.HeaderText <$> ["year"
                                              , "case_id"
                                              , "weight"
                                              , "weight_cumulative"
                                              , "st"
                                              , "dist_up"
                                              , "gender"
                                              , "age"
                                              , "educ"
                                              , "race"
                                              , "hispanic"
                                              , "pid3"
                                              , "pid7"
                                              , "pid3_leaner"
                                              , "vv_regstatus"
                                              , "vv_turnout_gvm"
                                              , "voted_rep_party"
                                              , "voted_pres_08"
                                              , "voted_pres_12"
                                              , "voted_pres_16"
                                              ])
ccesCols2020C :: S.Set FS.HeaderText
ccesCols2020C = S.insert (FS.HeaderText "voted_pres_20") ccesCols2018C

-- the things I would make Categorical are already ints. :(
ccesRowGen2020CAllCols = (FS.rowGen cces2020C_CSV) { FS.tablePrefix = "CCES"
                                                   , FS.separator   = ","
                                                   , FS.rowTypeName = "CCES"
                                                   }

ccesRowGen2018CAllCols = (FS.rowGen cces2018C_CSV) { FS.tablePrefix = "CCES"
                                                   , FS.separator   = ","
                                                   , FS.rowTypeName = "CCES"
                                                   }

ccesRowGen2018C = FS.modifyColumnSelector colSubset ccesRowGen2018CAllCols where
  colSubset = FS.columnSubset ccesCols2018C

ccesRowGen2020C = FS.modifyColumnSelector colSubset ccesRowGen2020CAllCols where
  colSubset = FS.columnSubset ccesCols2020C


ces2020CSV :: FilePath = dataDir ++ "CES20_Common_OUTPUT_vv.csv"

cesCols :: Int -> Int -> S.Set FS.HeaderText
cesCols yrSuffix congress = S.fromList (FS.HeaderText <$> ["caseid"
                                                                  , "commonpostweight"
                                                                  , "inputstate"
                                                                  , "cdid" <> show congress
                                                                  , "gender"
                                                                  , "birthyr"
                                                                  , "educ"
                                                                  , "race"
                                                                  , "hispanic"
                                                                  , "pid3"
                                                                  , "pid7"
                                                                  , "CL_voter_status" -- registration, Catalist
                                                                  , "CL_20" <> show yrSuffix <> "gvm" -- how voted and thus turnout, Catalist
                                                                  , "CC" <> show yrSuffix <> "_412" -- house candidate
                                                                  , "HouseCand1Party"
                                                                  , "HouseCand2Party"
                                                                  , "HouseCand3Party"
                                                                  , "HouseCand4Party"
                                           ])
cesRenames :: Bool -> Int -> Int -> Map FS.HeaderText FS.ColTypeName
cesRenames clAsInt yrSuffix congress =
  let nonCL = [ (FS.HeaderText "caseid", FS.ColTypeName "CaseId")
              , (FS.HeaderText "commonpostweight", FS.ColTypeName "Weight")
              , (FS.HeaderText "inputstate", FS.ColTypeName "StateFips")
              , (FS.HeaderText ("cdid" <> show congress), FS.ColTypeName "CD")
              , (FS.HeaderText ("CC" <> show yrSuffix <> "_412"), FS.ColTypeName "HouseVote")
              ]
      cl = if clAsInt
        then [(FS.HeaderText ("CL_20" <> show yrSuffix <> "gvm"), FS.ColTypeName "CTurnout")
             ,(FS.HeaderText ("CL_voter_status"), FS.ColTypeName "CVoterStatus")]
        else [(FS.HeaderText ("CL_20" <> show yrSuffix <> "gvm"), FS.ColTypeName "CTurnoutT")
             ,(FS.HeaderText ("CL_voter_status"), FS.ColTypeName "CVoterStatusT")]

  in M.fromList (nonCL <> cl)

--
unRenameHeader :: FS.HeaderText
               -> FS.HeaderText
               -> (S.Set FS.HeaderText, M.Map FS.HeaderText FS.ColTypeName)
               -> (S.Set FS.HeaderText, M.Map FS.HeaderText FS.ColTypeName)
unRenameHeader shouldBe is (cols, renames) = (newCols, newRenames) where
  asColTypeName (FS.HeaderText ht) = FS.ColTypeName ht
  newCols = S.insert is $ S.delete shouldBe cols
  newRenames = case M.lookup shouldBe renames of
    Nothing -> M.insert is (asColTypeName shouldBe) renames
    Just v -> M.insert is v $ M.delete shouldBe renames

addPresVote :: FS.HeaderText -> S.Set FS.HeaderText ->  Map FS.HeaderText FS.ColTypeName -> (S.Set FS.HeaderText, Map FS.HeaderText FS.ColTypeName)
addPresVote header cols renames = (S.insert header cols, M.insert header (FS.ColTypeName "PresVote") renames)

ccesRowGen2020AllCols = (FS.rowGen ces2020CSV) { FS.tablePrefix = "CES"
                                               , FS.separator   = ","
                                               , FS.rowTypeName = "CES20"
                                               }

-- "CC20_410" -- 2020 pres vote

cesRowGen2020 = FS.modifyColumnSelector modF ccesRowGen2020AllCols where
  (cols, renames) = addPresVote (FS.HeaderText "CC20_410") (cesCols 20 116) (cesRenames True 20 116)
  modF = FS.renameSomeUsingNames renames . FS.columnSubset cols

ces2018CSV :: FilePath = dataDir ++ "cces18_common_vv.csv"

ccesRowGen2018AllCols = (FS.rowGen ces2018CSV) { FS.tablePrefix = "CES"
                                               , FS.separator   = ","
                                               , FS.rowTypeName = "CES18"
                                               }

cesRowGen2018 = FS.modifyColumnSelector modF ccesRowGen2018AllCols where
  modF = FS.renameSomeUsingNames (cesRenames True 18 115) . FS.columnSubset (cesCols 18 115)

ces2016CSV :: FilePath = dataDir ++ "CCES16_Common_OUTPUT_Feb2018_VV.csv"

(cols2016, renames2016) = f (cesCols 16 115, cesRenames False 16 115) where
  f = unRenameHeader (FS.HeaderText "caseid") (FS.HeaderText "V101") .
      unRenameHeader (FS.HeaderText "CL_voter_status") (FS.HeaderText "CL_voterstatus") .
      unRenameHeader (FS.HeaderText "commonpostweight") (FS.HeaderText "commonweight_post") .
      unRenameHeader (FS.HeaderText "CL_2016gvm") (FS.HeaderText "CL_E2016GVM")

ccesRowGen2016AllCols = (FS.rowGen ces2016CSV) { FS.tablePrefix = "CES"
                                               , FS.separator   = ","
                                               , FS.rowTypeName = "CES16"
                                               }

cesRowGen2016 = FS.modifyColumnSelector modF ccesRowGen2016AllCols where
  (cols, renames) = addPresVote (FS.HeaderText "CC16_410a") cols2016 renames2016
  modF = FS.renameSomeUsingNames renames . FS.columnSubset cols
