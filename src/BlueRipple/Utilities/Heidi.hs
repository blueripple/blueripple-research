{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies     #-}

module BlueRipple.Utilities.Heidi
  (
    module BlueRipple.Utilities.Heidi
  )
where

import qualified Data.Set as Set
import qualified Data.Text as Text

import qualified Data.Foldable as Foldable

import qualified Heidi

type HRow = Heidi.Row Text Heidi.VP

--gatherSet :: (Functor f, Foldable f) => f Text -> Set Text
--gatherSet = Set.fromList . Foldable.toList

-- functions for gather/spread on typical row type
heidiColKey :: Text -> [Heidi.TC]
heidiColKey = pure . Heidi.mkTyN . toString

tcKeyToTextValue :: [Heidi.TC] -> Heidi.VP
tcKeyToTextValue tcs = Heidi.VPText $ Text.intercalate "_" $ fmap tcAsText tcs where
  tcAsText tc = let n = Heidi.tcTyN tc in toText $ if null n then Heidi.tcTyCon tc else n

gatherSet :: (Functor f, Foldable f) => [Heidi.TC] -> f Text -> Set [Heidi.TC]
gatherSet prefixTC = Set.fromList . Foldable.toList . fmap (\t -> reverse $ Heidi.mkTyN (toString t) : prefixTC)

rekeyCol :: Heidi.TrieKey k => k -> k -> Heidi.Row k v -> Either Text (Heidi.Row k v)
rekeyCol oldKey newKey r = case Heidi.lookup oldKey r of
  Nothing -> Left $ "old Key not found in BlueRipple.Utilities.Heidi.rekeyCol"
  Just v -> Right $ Heidi.insert newKey v $ Heidi.delete oldKey r
