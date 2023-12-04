{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE UnicodeSyntax #-}

module BlueRipple.Model.CategorizeElection where

import qualified Frames as F
import qualified Frames.Streamly.InCore as FI
import qualified Frames.Streamly.TH as FS
import qualified Frames.Transform as FT
import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V

FS.declareColumn "DistCategory" ''Text

data LeanRating = SafeR | LeanR | TiltR | Tossup | TiltD | LeanD | SafeD deriving stock (Eq, Ord, Show)

leanRatingText :: LeanRating -> Text
leanRatingText SafeR = "Safe R"
leanRatingText LeanR = "Lean R"
leanRatingText TiltR = "Tilt R"
leanRatingText Tossup = "Tossup"
leanRatingText SafeD = "Safe D"
leanRatingText LeanD = "Lean D"
leanRatingText TiltD = "Tilt D"

leanRating :: Double -> Double -> Double -> Double -> LeanRating
leanRating safe lean tilt x
  | x < 0.5 - safe = SafeR
  | x < 0.5 - lean = LeanR
  | x <= 0.5 - tilt = TiltR
  | x > 0.5 - tilt && x < 0.5 + tilt = Tossup
  | x > 0.5 + safe = SafeD
  | x > 0.5 + lean = LeanD
  | x >= 0.5 + tilt = TiltD
  | otherwise = error ("leanRating is missing a case! (safe="
                        <> show safe <> "; lean=" <> show lean <> "; tilt="
                        <> show tilt <> "; x=" <> show x <> ")"
                      )


ratingChangeText :: LeanRating -> LeanRating -> Text
ratingChangeText r1 r2 = if r1 == r2
                         then "No Change"
                         else leanRatingText r1 <> " -> " <> leanRatingText r2

pPLAndDPL :: LeanRating -> LeanRating -> Text
pPLAndDPL pplR dplR
  | pplR == SafeR && dplR < TiltR = "Safe R"
  | pplR == SafeR && dplR < LeanD = "Historically Safe R/Demographically close"
  | pplR == SafeR = "Historically Safe R/Demographically D!"
  | pplR == LeanR && dplR < TiltR = "Lean R"
  | pplR == LeanR && dplR < LeanD = "Historically Lean R/Demographically close"
  | pplR == LeanR = "Historically Lean R/Demographically D. Flippable?"
  | pplR == TiltR && dplR <= Tossup = "Tilt R"
  | pplR == TiltR = "Historically TiltR R/Demographically D. Flippable?"
  | pplR == Tossup && dplR < TiltR = "Historically tossup/Demographically R"
  | pplR == Tossup && dplR > TiltD = "Historically tossup/Demographically D"
  | pplR == Tossup = "Tossup"
  | pplR == SafeD && dplR > TiltD = "Safe D"
  | pplR == SafeD && dplR > LeanR = "Historically Safe D/Demographically close"
  | pplR == SafeD = "Historically Safe D/Demographically R!"
  | pplR == LeanD && dplR > TiltD = "Lean D"
  | pplR == LeanD && dplR < LeanR = "Historically Lean D/Demographically close"
  | pplR == LeanD = "Historically Lean D/Demographically R. Vulnerable?"
  | pplR == TiltD && dplR >= Tossup = "Tilt D"
  | pplR == TiltD = "Historically Tilt D/Demographically R. Vulnerable?"
  | otherwise = "Error! Missed a rating pair!"


addCategoriesToRecords :: FI.RecVec (rs V.++ '[DistCategory]) ⇒ [(Text, [F.Record rs])] → F.FrameRec (rs V.++ '[DistCategory])
addCategoriesToRecords = F.toFrame . concat . fmap f
  where
    f :: (Text, [F.Record rs]) -> [F.Record (rs V.++ '[DistCategory])]
    f (dc, rs) = fmap (V.<+> FT.recordSingleton @DistCategory dc) rs
